"""Bounded inline context with full, linked objects on disk for lazy recall."""
import json
from pathlib import Path


def materialize(store, roots, directory, budget=4000):
    if budget < 256:
        raise ValueError("Context budget must be at least 256 characters")
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    todo, seen, previews = list(roots), set(), []
    while todo:
        ref = todo.pop(0)
        key = (ref["kind"], ref["id"])
        if key in seen:
            continue
        seen.add(key)
        obj = store.recall(*key)
        obj["refs"] = [{**r, "file": str(directory / f"{r['kind']}-{r['id']}.json")} for r in obj["refs"]]
        path = directory / f"{key[0]}-{key[1]}.json"
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        if ref in roots:
            value = obj["value"]
            text = value.get("body", {}).get("text", value.get("summary", value.get("title", "")))
            previews.append({"kind": key[0], "id": key[1], "excerpt": text[:256], "file": str(path)})
        # ponytail: materialize this connected component for filesystem tools;
        # use an on-demand authenticated recall endpoint if graphs become large.
        todo.extend({"kind": r["kind"], "id": r["id"]} for r in obj["refs"])
    index = directory / "index.json"
    index.write_text(json.dumps(previews, ensure_ascii=False, indent=2), encoding="utf-8")
    inline = []
    result = {"index_file": str(index), "inline": inline}
    for preview in previews:
        inline.append(preview)
        if len(json.dumps(result, ensure_ascii=False)) > budget:
            inline.pop()
            break
    if len(json.dumps(result, ensure_ascii=False)) > budget:
        raise ValueError("Context budget cannot hold the recall index reference")
    return result
