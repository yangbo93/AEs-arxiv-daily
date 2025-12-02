import datetime
import requests
import json
import arxiv
import re
import os
import tempfile
import shutil
import glob

base_url = "https://arxiv.paperswithcode.com/api/v0/papers/"


def del_unicode(string):
    string = re.sub(r'\\u.{4}', '', string.__repr__())
    return string


def del_not_english(string):
    string = re.sub('[^A-Za-z]', '', string.__str__())
    return string


def get_authors(authors, first_author=False):
    # ensure we always return strings (avoid returning Author objects)
    if first_author:
        return str(authors[0]) if authors else ""
    return ", ".join(str(author) for author in authors)


def sort_papers(papers):
    output = dict()
    keys = list(papers.keys())
    keys.sort(reverse=True)
    for key in keys:
        output[key] = papers[key]
    return output


def safe_load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
            if not text:
                return {}
            return json.loads(text)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        # backup corrupted file and return empty dict so script can continue
        try:
            backup = f"{path}.corrupt.{int(datetime.datetime.now().timestamp())}"
            shutil.copyfile(path, backup)
            print(f"WARNING: JSON decode error reading {path}. Backed up to {backup}. Reinitializing.")
        except Exception as e:
            print(f"WARNING: Failed to backup corrupted json {path}: {e}")
        return {}


def safe_write_json(path, data):
    dirpath = os.path.dirname(path) or "."
    fd, tmp_path = tempfile.mkstemp(prefix="tmp", dir=dirpath)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def get_daily_papers(topic, query="AEs", max_results=2):
    """
    @param topic: str
    @param query: str
    @return paper_with_code: dict
    """

    # output
    content = dict()
    content_to_web = dict()

    search_engine = arxiv.Search(
        query=query,
        max_results=max_results,
        sort_by=arxiv.SortCriterion.SubmittedDate
    )

    cnt = 0

    for result in search_engine.results():

        paper_id = result.get_short_id()
        paper_title = result.title
        paper_url = result.entry_id

        code_url = base_url + paper_id
        paper_abstract = result.summary.replace("\n", " ")
        paper_authors = get_authors(result.authors)
        paper_first_author = get_authors(result.authors, first_author=True)
        primary_category = result.primary_category

        publish_time = result.published.date()
        update_time = result.updated.date()

        print("Time = ", update_time,
              " title = ", paper_title,
              " author = ", paper_first_author)

        # eg: 2108.09112v1 -> 2108.09112
        ver_pos = paper_id.find('v')
        if ver_pos == -1:
            paper_key = paper_id
        else:
            paper_key = paper_id[0:ver_pos]

        try:
            resp = requests.get(code_url, timeout=10)
            if resp.status_code == 200:
                try:
                    r = resp.json()
                except Exception:
                    r = {}
            else:
                r = {}
            # source code link
            if isinstance(r, dict) and r.get("official"):
                cnt += 1
                repo_url = r["official"].get("url", "")
                content[paper_key] = f"|**{update_time}**|**{paper_title}**|{paper_first_author} et.al.|[{paper_id}]({paper_url})|**[link]({repo_url})**|\n"
                content_to_web[paper_key] = f"- **{update_time}**, **{paper_title}**, {paper_first_author} et.al., [PDF:{paper_id}]({paper_url}), **[code]({repo_url})**\n"
            else:
                content[paper_key] = f"|**{update_time}**|**{paper_title}**|{paper_first_author} et.al.|[{paper_id}]({paper_url})|null|\n"
                content_to_web[paper_key] = f"- **{update_time}**, **{paper_title}**, {paper_first_author} et.al., [PDF:{paper_id}]({paper_url})\n"

        except Exception as e:
            print(f"exception: {e} with id: {paper_key}")

    data = {topic: content}
    data_web = {topic: content_to_web}
    return data, data_web


def update_json_file(filename, data_all):
    """
    Read filename safely, merge data_all into it, and write it back atomically.
    data_all is a list of dicts like [{topic: {paper_key: md_line}}...]
    """
    json_data = safe_load_json(filename)

    # update papers in each keywords
    for data in data_all:
        for keyword in data.keys():
            papers = data[keyword]
            if keyword in json_data:
                # ensure both sides are dicts
                if isinstance(json_data[keyword], dict):
                    json_data[keyword].update(papers)
                else:
                    json_data[keyword] = papers
            else:
                json_data[keyword] = papers

    safe_write_json(filename, json_data)


def merge_json_files(file_list):
    merged = {}
    for p in file_list:
        d = safe_load_json(p)
        for k, v in d.items():
            if k not in merged:
                merged[k] = {}
            if isinstance(v, dict):
                merged[k].update(v)
    return merged


def write_md_from_data(data, to_web=False, md_filename=None):
    DateNow = datetime.date.today()
    DateNow = str(DateNow).replace('-', '.')

    if md_filename is None:
        md_filename = "./docs/index.md" if to_web else "README.md"

    # write content atomically via temp file
    dirpath = os.path.dirname(md_filename) or "."
    fd, tmp_path = tempfile.mkstemp(prefix="tmpmd", dir=dirpath)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            if to_web:
                f.write("---\nlayout: default\n---\n\n")
            f.write("## Updated on " + DateNow + "\n\n")

            for keyword in data.keys():
                day_content = data[keyword]
                if not day_content:
                    continue
                f.write(f"## {keyword}\n\n")
                if not to_web:
                    f.write("|Publish Date|Title|Authors|PDF|Code|\n|---|---|---|---|---|\n")
                else:
                    f.write("| Publish Date | Title | Authors | PDF | Code |\n")
                    f.write("|:---------|:-----------------------|:---------|:------|:------|\n")

                # sort papers by key (descending)
                day_content = sort_papers(day_content)
                for _, v in day_content.items():
                    if v is not None:
                        f.write(v)
                f.write("\n")
        # atomic replace
        os.replace(tmp_path, md_filename)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


if __name__ == "__main__":
    data_collector = []
    data_collector_web = []

    keywords = dict()
    # fix: ensure spaces around boolean operators for arXiv query syntax
    keywords["Adversarial Examples"] = 'AEs OR "adversarial examples"'
    keywords["Generalization"] = 'model training AND "Generalization"'

    for topic, keyword in keywords.items():
        print("Keyword: " + topic)
        data, data_web = get_daily_papers(topic, query=keyword, max_results=10)
        data_collector.append(data)
        data_collector_web.append(data_web)
        print("\n")

    # determine 4-month rolling period id (e.g., 2025-p1, 2025-p2, 2025-p3)
    today = datetime.date.today()
    year = today.year
    period_index = (today.month - 1) // 4 + 1
    period = f"{year}-p{period_index}"

    # file names for this period
    root_json = f"AEs-arxiv-daily-{period}.json"
    docs_json = f"./docs/AEs-arxiv-daily-web-{period}.json"

    # ensure docs dir exists
    os.makedirs("./docs", exist_ok=True)

    # --- Backfill legacy files if present and period files do not exist ---
    legacy_root = "AEs-arxiv-daily.json"
    legacy_docs = "./docs/AEs-arxiv-daily-web.json"
    if not os.path.exists(root_json) and os.path.exists(legacy_root):
        try:
            shutil.copyfile(legacy_root, root_json)
            print(f"INFO: Backed up legacy {legacy_root} -> {root_json}")
        except Exception as e:
            print(f"WARNING: failed to copy legacy root json: {e}")
    if not os.path.exists(docs_json) and os.path.exists(legacy_docs):
        try:
            shutil.copyfile(legacy_docs, docs_json)
            print(f"INFO: Backed up legacy {legacy_docs} -> {docs_json}")
        except Exception as e:
            print(f"WARNING: failed to copy legacy docs json: {e}")
    # ------------------------------------------------------------------

    # update per-period JSON files
    update_json_file(root_json, data_collector)
    update_json_file(docs_json, data_collector_web)

    # generate aggregated md files from all period JSONs so the md shows full history
    root_json_files = sorted(glob.glob("AEs-arxiv-daily-*.json"))
    docs_json_files = sorted(glob.glob("./docs/AEs-arxiv-daily-web-*.json"))

    merged_root = merge_json_files(root_json_files)
    merged_docs = merge_json_files(docs_json_files)

    # write README.md (aggregate across all root jsons)
    write_md_from_data(merged_root, to_web=False, md_filename="README.md")
    # write docs/index.md (aggregate across all docs jsons)
    write_md_from_data(merged_docs, to_web=True, md_filename="./docs/index.md")

    print("finished")
