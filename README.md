# CPOS lead feed

The CPOS outreach agent set up this repo by itself and keeps it up to date. Nothing here needs you.

**What it does.** On Sunday and Wednesday mornings, and whenever the agent runs low on leads, GitHub runs
`extract.py`. It finds Canadian businesses that are likely to take card payments in person, scores them and saves a
batch in `feed/history/`. The agent adds each batch to its Leads tab once.

- **Where it looks:** every city, town and village in Canada, from Overture Maps' own list of places (no fixed city
  list). Ottawa first, then the rest of Ontario, then the rest of the country, by the shares set in the agent's Settings
  tab (`prospect_mix`). Big cities are searched in squares; small towns get their own share. Quebec is counted but
  not searched until outreach in French is set up.
- **What it looks for:** every Overture business type. `model/category_rules.json` estimates how likely each type is
  to take card payments face to face, and which terminal fits (countertop, portable or mobile). Types the model does
  not know yet are tried a few at a time.
- **How it ranks:** each business gets a score from 0 to 100 with its reasons, marked `[estimate]` (from the model),
  `[listing]` (from the business's own listing) or `[learned]` (from the agent's results). The best go first, but every
  viable kind of business and region keeps a share, so lower ranked businesses are still reached. Turns go by kind of
  business (the model's rules), then by Overture type within it, because Overture splits restaurants into dozens of
  types and most other kinds into one or two. A listing with no category is judged by its name, and says so.
- **How it learns:** the agent writes `state/feedback.json` (emails found, replies, per business type and market).
  Types and places that work move up, the others move down, never to zero. A type that does well leads to its related
  types being tried.
- **New businesses first:** once per Overture release (about monthly), businesses in the Ottawa area that were not in the
  last release get a higher score and go first, since a place that just opened is still choosing how it takes cards.
  `state/seed_ids.json` keeps the area's ids (hashed) to compare with the next release.
- **No duplicates:** `state/sent_ids.txt` and `state/sent_keys.txt` remember every business handed over, and
  `state/known_keys.txt` (written by the agent, hashed) lists businesses the agent already has.
- **Coverage:** `feed/COVERAGE.md` and `feed/coverage.json` show what has been searched per province and business
  type, and the gaps. `feed/report/latest.json` describes the last run.
- **Settings:** `config.json` is written by the agent from its Settings tab. To run by hand: Actions > Lead feed >
  Run workflow (you can set how many businesses and which provinces).

Map data: Overture Maps Foundation (places CDLA Permissive 2.0, divisions ODbL).
