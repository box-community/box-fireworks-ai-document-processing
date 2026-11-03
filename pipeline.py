"""
Box -> Fireworks document-classification pipeline.

The same steps as box_to_fireworks_poc_script, restructured so a UI can watch
them happen: every stage reports progress through an `emit` callback instead of
printing, and the run can be cancelled between files.

Credentials are read from the environment only. Nothing here accepts a secret
from an HTTP request, and no secret is ever placed in an emitted event.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
import time
from collections import Counter, OrderedDict
from pathlib import Path
from typing import Callable, Optional

API = "https://api.box.com/2.0"

IMAGE_EXTS = {"png", "jpg", "jpeg", "tif", "tiff"}
DOC_EXTS = {"pdf", "doc", "docx", "ppt", "pptx", "xls", "xlsx",
            "csv", "txt", "rtf", "md", "html"}

# ---- taxonomy -------------------------------------------------------------- #
# This block is the only place the label taxonomy is defined. The server reads
# it, serves it to the UI, and uses it as the default allowlist for a run, so a
# different use case only needs to change these three values.
#
# The example folders below are departments; the labels can be any folder names
# (document types, business units, retention classes). LABEL_NOUN is how the
# training prompt refers to them.

LABEL_NOUN = "department"
LABEL_NOUN_PLURAL = "departments"

LABEL_DEFINITIONS = OrderedDict([
    ("Corporate Legal", "contracts, NDAs, and legal agreements"),
    ("Finance", "financial reports, earnings, and analyses"),
    ("HR", "hiring, onboarding, and employee documents"),
    ("Marketing", "campaigns, creative briefs, and press releases"),
    ("Sales", "sales contracts, invoices, and proposals"),
])

DEFAULT_ALLOWED_LABELS = list(LABEL_DEFINITIONS)

# Prefix for the Fireworks dataset id; a run appends a timestamp.
DATASET_PREFIX = "docproc"

STAGES = ["connect", "list", "extract", "curate", "redact", "label", "dataset", "train"]


class Cancelled(Exception):
    pass


class PipelineError(Exception):
    pass


def load_env(path=".env"):
    """Minimal .env loader so the server has no hard dependency on python-dotenv."""
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def env_status():
    """Which credentials are present. Booleans only — never the values."""
    need = ["BOX_CLIENT_ID", "BOX_CLIENT_SECRET", "BOX_ENTERPRISE_ID",
            "FIREWORKS_API_KEY", "FIREWORKS_ACCOUNT_ID"]
    present = {k: bool(os.environ.get(k)) for k in need}
    return {
        "present": present,
        "boxReady": all(present[k] for k in need[:3]),
        "fireworksReady": all(present[k] for k in need[3:]),
        "defaultSource": os.environ.get("BOX_SHARED_LINK", ""),
    }


class Pipeline:
    def __init__(self, cfg: dict, emit: Callable[[dict], None], should_stop=lambda: False):
        self.cfg = cfg
        self._emit = emit
        self.should_stop = should_stop
        self.session = None
        self.headers = {}
        self.files = []
        self.docs = []

    # ---------- plumbing ----------

    def emit(self, **ev):
        self._emit(ev)

    def log(self, msg, level="info"):
        self.emit(t="log", level=level, msg=str(msg))

    def stage(self, name, status, **extra):
        self.emit(t="stage", stage=name, status=status, **extra)

    def check(self):
        if self.should_stop():
            raise Cancelled()

    # ---------- 1. connect ----------

    def connect(self):
        self.stage("connect", "running")
        try:
            import requests
            from box_sdk_gen import BoxCCGAuth, CCGConfig
        except ImportError as e:
            raise PipelineError(
                f"Missing dependency ({e.name}). Install them with: "
                "pip install -r requirements.txt")

        cid = os.environ.get("BOX_CLIENT_ID")
        secret = os.environ.get("BOX_CLIENT_SECRET")
        ent = os.environ.get("BOX_ENTERPRISE_ID")
        if not (cid and secret and ent):
            raise PipelineError("Box credentials missing. Set BOX_CLIENT_ID, "
                                "BOX_CLIENT_SECRET and BOX_ENTERPRISE_ID in .env")

        # Client Credentials Grant gives a service-account token, so no developer
        # token has to be pasted in. The token is fetched once and pinned to this
        # run's headers; a run longer than the token lifetime would need to
        # re-request it (see Known limitations in the README).
        auth = BoxCCGAuth(config=CCGConfig(client_id=cid, client_secret=secret,
                                           enterprise_id=ent))
        token = auth.retrieve_token().access_token
        self.session = requests.Session()
        self.headers = {"Authorization": f"Bearer {token}"}

        source = (self.cfg.get("source") or os.environ.get("BOX_SHARED_LINK") or "").strip()
        if not source:
            raise PipelineError("No folder given. Paste a Box shared link or a folder id.")

        if source.startswith("http"):
            # The BoxApi header lets the service account read a shared folder
            # without being added as a collaborator on it.
            self.headers["BoxApi"] = f"shared_link={source}"
            r = self.session.get(f"{API}/shared_items", headers=self.headers, timeout=60)
            if not r.ok:
                raise PipelineError(f"Could not resolve that shared link ({r.status_code}). "
                                    "Check the link is still active and open to your app.")
            root = r.json()
            if root.get("type") != "folder":
                raise PipelineError("That shared link points at a file, not a folder.")
            folder_id, folder_name = root["id"], root.get("name") or "Shared folder"
        elif re.fullmatch(r"\d+", source):
            r = self.session.get(f"{API}/folders/{source}", headers=self.headers, timeout=60)
            if not r.ok:
                raise PipelineError(
                    f"Folder {source} is not reachable ({r.status_code}). A service account "
                    "starts with an empty folder tree — invite it as a collaborator, or use a "
                    "shared link instead.")
            folder_id, folder_name = source, r.json().get("name") or source
        else:
            raise PipelineError("Enter a Box shared link (https://…/s/…) or a numeric folder id.")

        self.folder_id, self.folder_name = folder_id, folder_name
        self.emit(t="folder", name=folder_name, id=folder_id)
        self.stage("connect", "done", detail=folder_name)

    # ---------- 2. list ----------

    def list_files(self, folder_id, path=""):
        out, offset = [], 0
        while True:
            self.check()
            r = self.session.get(
                f"{API}/folders/{folder_id}/items", headers=self.headers,
                params={"limit": 200, "offset": offset,
                        "fields": "type,id,name,extension"}, timeout=60).json()
            for e in r.get("entries", []):
                child = f"{path}/{e['name']}"
                if e["type"] == "folder":
                    out += self.list_files(e["id"], child)
                else:
                    out.append({"id": e["id"], "name": e["name"], "path": child,
                                "extension": (e.get("extension") or "").lower()})
            offset += 200
            if offset >= r.get("total_count", 0):
                return out

    def list_stage(self):
        self.stage("list", "running")
        self.files = self.list_files(self.folder_id)
        prefix = (self.cfg.get("subpath") or "").strip().strip("/")
        if prefix:
            self.files = [f for f in self.files
                          if f["path"].lstrip("/").startswith(prefix + "/")
                          or f["path"].lstrip("/") == prefix]
            self.log(f"filtered to /{prefix}: {len(self.files)} files")
        if not self.files:
            raise PipelineError("No files found in that folder.")
        self.stage("list", "done", count=len(self.files))

    # ---------- 3. extract ----------

    def text_representation(self, file_id, retries=3):
        import requests
        h = {**self.headers, "X-Rep-Hints": "[extracted_text]"}
        for attempt in range(retries):
            try:
                r = self.session.get(f"{API}/files/{file_id}", headers=h,
                                     params={"fields": "representations"}, timeout=60)
                r.raise_for_status()
                reps = r.json().get("representations", {}).get("entries", [])
                if not reps:
                    return "", "no_representation"
                rep = reps[0]
                for _ in range(15):  # the representation is generated on demand
                    self.check()
                    state = self.session.get(rep["info"]["url"], headers=self.headers,
                                             timeout=60).json()["status"]["state"]
                    if state == "success":
                        break
                    time.sleep(1.5)
                url = rep["content"]["url_template"].replace("{+asset_path}", "")
                return self.session.get(url, headers=self.headers, timeout=60).text, "ok"
            except requests.HTTPError as e:
                code = e.response.status_code if e.response is not None else None
                if code in (403, 404):
                    return "", f"http_{code}"      # policy block — skip, don't retry
                time.sleep(2 ** attempt)
            except requests.RequestException:
                time.sleep(2 ** attempt)
        return "", "failed"

    def ocr_text(self, file_id):
        body = {"items": [{"type": "file", "id": file_id}],
                "fields": [{"key": "text", "type": "string",
                            "description": "Transcribe all readable text verbatim. "
                                           "Output only the text."}]}
        r = self.session.post(f"{API}/ai/extract_structured",
                              headers={**self.headers, "Content-Type": "application/json"},
                              json=body, timeout=120)
        if not r.ok:
            return "", f"http_{r.status_code}"
        return r.json().get("answer", {}).get("text", ""), "ok"

    def extract(self):
        self.stage("extract", "running", total=len(self.files))
        total = len(self.files)
        for i, f in enumerate(self.files, 1):
            self.check()
            ext = f["extension"]
            if ext in IMAGE_EXTS:
                f["text"], f["status"] = self.ocr_text(f["id"])
            elif ext in DOC_EXTS:
                f["text"], f["status"] = self.text_representation(f["id"])
            else:
                f["text"], f["status"] = "", "skipped_unsupported"
            parts = [p for p in f["path"].split("/") if p]
            self.emit(t="file", i=i, total=total, path=f["path"], name=f["name"],
                      folder=parts[0] if len(parts) > 1 else "(root)",
                      ext=ext, status=f["status"], chars=len(f["text"]))
        counts = Counter(f["status"] for f in self.files)
        self.stage("extract", "done",
                   total=total,
                   ok=counts.get("ok", 0),
                   blocked=counts.get("http_403", 0),
                   unsupported=counts.get("skipped_unsupported", 0),
                   nonEmpty=sum(1 for f in self.files if f["text"]))

    # ---------- 4. curate ----------

    def curate(self):
        self.stage("curate", "running")
        min_chars = int(self.cfg.get("minChars", 200))
        seen, docs, dropped = set(), [], []
        for f in self.files:
            text = f["text"].strip()
            if len(text) < min_chars:
                dropped.append({"reason": f"short({len(text)}c)", "path": f["path"]})
                continue
            h = hashlib.sha256(text.encode()).hexdigest()
            if h in seen:
                dropped.append({"reason": "duplicate", "path": f["path"]})
                continue
            seen.add(h)
            docs.append({**f, "text": text})
        self.docs = docs
        self.stage("curate", "done", input=len(self.files), usable=len(docs),
                   dropped=dropped, minChars=min_chars)

    # ---------- 5. redact ----------

    def redact(self):
        self.stage("redact", "running", total=len(self.docs))
        try:
            from presidio_analyzer import AnalyzerEngine
            from presidio_analyzer.nlp_engine import NlpEngineProvider
            from presidio_anonymizer import AnonymizerEngine
        except ImportError:
            raise PipelineError("Presidio is not installed. "
                                "pip install presidio-analyzer presidio-anonymizer")

        model = os.environ.get("SPACY_MODEL", "en_core_web_sm")
        nlp = NlpEngineProvider(nlp_configuration={
            "nlp_engine_name": "spacy",
            "models": [{"lang_code": "en", "model_name": model}],
        }).create_engine()
        analyzer = AnalyzerEngine(nlp_engine=nlp)
        anonymizer = AnonymizerEngine()

        sample = None
        total_spans = 0
        entity_counts = Counter()

        for i, d in enumerate(self.docs, 1):
            self.check()
            results = analyzer.analyze(text=d["text"], language="en")
            clean = anonymizer.anonymize(text=d["text"], analyzer_results=results).text
            for r in results:
                entity_counts[r.entity_type] += 1
            total_spans += len(results)
            if sample is None and results:
                # Keep one short before/after so the UI can show the real transformation.
                span = results[0]
                start = max(0, span.start - 160)
                end = min(len(d["text"]), span.end + 160)
                sample = {
                    "path": d["path"],
                    "before": d["text"][start:end],
                    "after": clean[start:min(len(clean), end + 40)],
                    "entities": sorted({r.entity_type for r in results}),
                }
            d["text"] = clean
            d["_redacted"] = True
            self.emit(t="redact", done=i, total=len(self.docs), spans=total_spans)

        self.stage("redact", "done", docs=len(self.docs), spans=total_spans,
                   engine="Microsoft Presidio", nlp=model, local=True,
                   entities=dict(entity_counts.most_common()), sample=sample)

    # ---------- 6. label ----------

    @staticmethod
    def label_of(path):
        parts = [p for p in path.split("/") if p]
        return parts[0] if len(parts) > 1 else "Other"

    def label(self):
        self.stage("label", "running")
        allowed = self.cfg.get("allowedLabels")
        if allowed:
            allowed = set(allowed)
            dropped = [{"reason": self.label_of(d["path"]), "path": d["path"]}
                       for d in self.docs if self.label_of(d["path"]) not in allowed]
            kept = [d for d in self.docs if self.label_of(d["path"]) in allowed]
        else:
            dropped, kept = [], list(self.docs)
        before = len(self.docs)
        self.docs = kept
        for d in self.docs:
            d["label"] = self.label_of(d["path"])
        order = sorted({d["label"] for d in self.docs})
        counts = OrderedDict(Counter(d["label"] for d in self.docs).most_common())
        definitions = self.cfg.get("labelDefinitions") or LABEL_DEFINITIONS
        self.definitions = {k: v for k, v in definitions.items() if k in order}
        self.label_order = order
        self.stage("label", "done", kept=len(self.docs), of=before, order=order,
                   counts=counts, dropped=dropped, definitions=self.definitions)

    # ---------- 7. dataset ----------

    def build_dataset(self):
        self.stage("dataset", "running")
        if not self.docs:
            raise PipelineError("Nothing left to train on after labelling.")
        if any(not d.get("_redacted") for d in self.docs):
            raise PipelineError("Safety check failed: refusing to build a dataset from unredacted text.")
        max_chars = int(self.cfg.get("maxChars", 10000))
        label_lines = "\n".join(
            f"- {l}: {self.definitions[l]}" if l in self.definitions else f"- {l}"
            for l in self.label_order)
        noun = self.cfg.get("labelNoun") or LABEL_NOUN
        plural = self.cfg.get("labelNounPlural") or LABEL_NOUN_PLURAL
        system = ("You are a document classifier. Classify the document into exactly one of "
                  f"these {plural}:\n" + label_lines +
                  f"\n\nRespond with only the {noun} name.")
        self.system = system

        random.seed(13)
        random.shuffle(self.docs)
        out_dir = Path(self.cfg.get("outDir", "runs"))
        out_dir.mkdir(parents=True, exist_ok=True)
        train_file = out_dir / f"{self.cfg['runId']}-train.jsonl"
        with train_file.open("w", encoding="utf-8") as fh:
            for d in self.docs:
                fh.write(json.dumps({"messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": d["text"][:max_chars]},
                    {"role": "assistant", "content": d["label"]},
                ]}, ensure_ascii=False) + "\n")

        self.train_file = train_file
        preview = {
            "label": self.docs[0]["label"],
            "path": self.docs[0]["path"],
            "excerpt": self.docs[0]["text"][:340],
        }
        self.stage("dataset", "done", examples=len(self.docs), file=train_file.name,
                   system=system, maxChars=max_chars, preview=preview)

    # ---------- 8. train ----------

    def train(self):
        if not self.cfg.get("train"):
            self.stage("train", "skipped",
                       detail="Stopped before upload. Nothing left the machine.")
            return
        self.stage("train", "running")
        key = os.environ.get("FIREWORKS_API_KEY")
        account = os.environ.get("FIREWORKS_ACCOUNT_ID")
        if not (key and account):
            raise PipelineError("Fireworks credentials missing. Set FIREWORKS_API_KEY "
                                "and FIREWORKS_ACCOUNT_ID in .env")
        from fireworks import Fireworks, file_from_path

        fw = Fireworks(api_key=key, account_id=account)
        prefix = self.cfg.get("datasetPrefix") or DATASET_PREFIX
        dataset_id = f"{prefix}-{int(time.time())}"
        self.log(f"creating dataset {dataset_id}")
        fw.datasets.create(dataset_id=dataset_id, dataset={
            "format": "CHAT", "example_count": str(len(self.docs)), "user_uploaded": {}})
        fw.datasets.upload(dataset_id, file=file_from_path(str(self.train_file)))
        # Dataset states are STATE_UNSPECIFIED / UPLOADING / READY — there is no
        # terminal failure state, so a stuck upload can only be caught by a deadline.
        timeout = int(self.cfg.get("datasetTimeout", 600))
        deadline = time.time() + timeout
        state = None
        while time.time() < deadline:
            self.check()
            state = fw.datasets.get(dataset_id).state
            if state == "READY":
                break
            time.sleep(3)
        if state != "READY":
            raise PipelineError(
                f"Dataset {dataset_id} was still {state or 'unknown'} after {timeout}s. "
                "It was uploaded but never became READY; check it in the Fireworks "
                "dashboard and delete it if it is not usable.")
        self.dataset_id = dataset_id
        self.emit(t="dataset_ready", id=dataset_id)

        base = self.cfg.get("baseModel", "qwen3p5-9b")
        epochs = int(self.cfg.get("epochs", 1))
        job = fw.supervised_fine_tuning_jobs.create(
            dataset=f"accounts/{account}/datasets/{dataset_id}",
            base_model=f"accounts/fireworks/models/{base}",
            epochs=epochs,
        )
        self.log(f"job {job.name}")
        job_id = job.name.split("/")[-1]
        state = "JOB_STATE_UNSPECIFIED"
        deadline = time.time() + int(self.cfg.get("pollSeconds", 900))
        while time.time() < deadline:
            self.check()
            j = fw.supervised_fine_tuning_jobs.get(job_id)
            state = j.state
            self.emit(t="train_state", state=state, job=job.name,
                      outputModel=j.output_model)
            if state in ("JOB_STATE_COMPLETED", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED"):
                break
            time.sleep(10)

        self.stage("train", "done", job=job.name, outputModel=job.output_model,
                   baseModel=base, epochs=epochs, state=state, method="LoRA SFT",
                   datasetId=dataset_id)

    # ---------- orchestration ----------

    def run(self):
        started = time.time()
        self.connect()
        self.list_stage()
        self.extract()
        self.curate()
        self.redact()
        self.label()
        self.build_dataset()
        self.train()
        self.emit(t="finished", seconds=round(time.time() - started, 1))


def run_pipeline(cfg, emit, should_stop=lambda: False):
    """Run to completion, translating failures into a terminal event."""
    p = Pipeline(cfg, emit, should_stop)
    try:
        p.run()
        return "completed"
    except Cancelled:
        emit({"t": "cancelled"})
        return "cancelled"
    except PipelineError as e:
        emit({"t": "error", "msg": str(e)})
        return "failed"
    except Exception as e:  # noqa: BLE001 - surface anything else to the UI too
        emit({"t": "error", "msg": f"{type(e).__name__}: {e}"})
        return "failed"
