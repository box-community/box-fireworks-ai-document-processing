# Document Processing — Box to Fireworks

This self-contained package runs a document-classification pipeline through a
local browser UI. It recursively reads a Box folder, extracts text, curates and
redacts documents locally, builds a chat-format dataset, and optionally uploads
that redacted dataset to Fireworks for supervised fine-tuning.

The shipped example classifies documents into departments by folder name. The
labels and the task are configurable — see **Labels** and **Task scope**.

## Prerequisites

- Python 3.10 or newer
- A Box Custom App configured for Client Credentials Grant, with access to the
  source folder or shared link
- Fireworks credentials only if **Upload & train** will be enabled

## Setup and run

```bash
cd document-processing
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm
cp .env.example .env
```

Replace the required placeholders in `.env`, then start the local server:

```bash
python3 server.py
```

Open `http://127.0.0.1:8765`, enter a Box shared link or numeric folder ID, and
select **Run**. Leave **Upload & train** unchecked to stop after creating the
local dataset; no document text is sent to Fireworks in that mode.

`BOX_SHARED_LINK` is empty in `.env.example` — there is no built-in folder. If
you set it in `.env` to a shared-link URL or numeric folder ID, leaving the UI
source field blank uses that value. `SPACY_MODEL` defaults to `en_core_web_sm`
when unset.

## Pipeline behavior

1. Connect to Box using the service account.
2. Recursively list files, optionally restricted to a subfolder.
3. Extract supported document text with Box Text Representations; use Box AI
   extraction for supported images.
4. Drop short documents and exact-text duplicates.
5. Redact detected identifiers locally with Presidio and spaCy.
6. Keep documents whose top-level folder matches the configured label list
   (see **Labels** below).
7. Write a chat-format JSONL dataset under `runs/`.
8. If explicitly enabled, upload that dataset and start Fireworks LoRA SFT.

## Labels

The five departments in this package (`Corporate Legal`, `Finance`, `HR`,
`Marketing`, `Sales`) are an example taxonomy, not a product constraint. The
labels can be any folder names — document types, business units, retention
classes, matter types.

Each document's label is the name of its top-level Box folder, and the
allowlist keeps only those folders. Root-level files are labeled `Other` and
dropped unless you add `Other` to the list.

The label always comes from the **first** path segment. If your taxonomy sits
deeper in the tree (`/Region/Finance/...`), every document is labeled `Region`.
The **Subfolder** field filters which files are read; it does not rebase paths,
so it cannot move the taxonomy up a level. Either point the run at a folder
whose immediate children are the taxonomy, or change `label_of` in
`pipeline.py` to pick the segment you want.

To use a different taxonomy, edit the taxonomy block at the top of
`pipeline.py`:

- `LABEL_DEFINITIONS` — the labels and a short description of each
- `LABEL_NOUN` / `LABEL_NOUN_PLURAL` — how the training prompt refers to them,
  for example `document type` / `document types`

That block is the only definition. The server serves it to the UI at
`/api/config` and uses it as the default allowlist, so nothing else needs to
change. An API client can also send `allowedLabels`, `labelDefinitions`,
`labelNoun`, or `labelNounPlural` on `/api/run` to override a single run.

## Task scope

This package is a deliberately simple example: single-label classification of
whole documents, where the label is a folder name and each training example is
one document. It is meant to prove the Box-to-Fireworks path end to end.

More complex tasks — extracting structured fields, multi-label tagging,
per-page or per-clause labeling, or labels that come from Box metadata rather
than folders — use the same connect, extract, curate, and redact stages, but
need a different system prompt and a different assistant target in the dataset
step (`build_dataset` in `pipeline.py`). Treat this as a starting point for
those, not a finished product.

## Known limitations

Accepted for a PoC; worth fixing before anything resembling production use.

- **Labels come from the first path segment only.** See **Labels** above.
- **The Box token is pinned for the run.** Client Credentials Grant returns a
  token once at connect time and every later call reuses it. A corpus large
  enough to run past the token lifetime will start failing partway through
  extraction. Re-request the token inside the extract loop for long runs.
- **Representation readiness is not enforced.** `text_representation` polls a
  document's representation for a bounded time and then downloads it anyway,
  without checking the download status. A representation that is still
  generating can be stored as text, so a very large or slow file can land in
  the dataset with placeholder or partial content. Spot-check extracted text
  on a first run against a new corpus.
- **The reported output model comes from job creation,** not the final poll, so
  the Train screen shows the model id assigned at submit time.

## Data boundary and artifacts

Fireworks datasets are named `docproc-<timestamp>`; override `DATASET_PREFIX`
in `pipeline.py` or send `datasetPrefix` on `/api/run` to change that. After
upload the run waits for the dataset to reach `READY` and fails with a clear
error after `datasetTimeout` seconds (default 600) rather than waiting forever.

The pipeline replaces each document's working text with the Presidio output
before dataset construction. Dataset creation refuses any document that has not
passed that redaction step. Fireworks receives only the generated redacted
JSONL, and only when **Upload & train** is checked.

Box extraction necessarily sends requests to Box. Redaction itself runs on the
machine hosting this package. The local UI receives progress and a short
before/after sample; treat the browser session and generated `runs/` directory
as customer data.

`runs/` is created automatically when the server starts. It contains generated
training JSONL and saved run events. Those saved runs can be replayed from the
UI after at least one suitable live run; no fixture or static replay data ships
with this package. Delete `runs/` when its artifacts are no longer needed.
Deleting a local run does not delete a Fireworks dataset.

> **Note:** Saved run files in `runs/*.json` are not fully redacted. To power
> the before/after view, the redact step keeps one short excerpt (about 320
> characters) of a real document as it was *before* redaction, and saved events
> also include source file paths and the Box folder name and ID. `runs/` is
> gitignored, but these files still hold unredacted customer data on local disk.
> Handle them as your task requires — for example, restrict access to the
> machine, delete runs after use, exclude `runs/` when copying or sharing this
> folder, or change `Pipeline.redact` in `pipeline.py` to stop keeping the
> pre-redaction excerpt.

## Configuration

Required for Box:

- `BOX_CLIENT_ID`
- `BOX_CLIENT_SECRET`
- `BOX_ENTERPRISE_ID`

Required only for Fireworks upload/training:

- `FIREWORKS_API_KEY`
- `FIREWORKS_ACCOUNT_ID`

Optional:

- `BOX_SHARED_LINK` — default source when the UI folder field is empty; unset
  means the operator must paste a link or folder ID
- `SPACY_MODEL`

Credentials are loaded only from `.env`. They are not accepted in HTTP request
bodies, emitted as run events, or persisted in saved run configuration.

## Supported input

Documents: PDF, Word, PowerPoint, Excel, CSV, text, RTF, Markdown, and HTML.
Images: PNG, JPEG, and TIFF. Unsupported files are logged and skipped. Box
policy failures are logged and not retried.

## Package files

- `app.html` — local UI
- `server.py` — HTTP API, server-sent events, run persistence, and replay
- `pipeline.py` — Box extraction, curation, redaction, dataset, and training
- `requirements.txt` — Python dependencies
- `.env.example` — placeholder-only configuration template
- `.gitignore` — excludes credentials, environments, caches, and run artifacts
