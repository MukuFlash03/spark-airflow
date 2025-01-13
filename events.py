from serpapi import GoogleSearch

params = {
  "engine": "google_events",
  "q": "Events in Austin",
  "hl": "en",
  "gl": "us",
  "api_key": "81d939b26b8ced30b7c0a49b77aa3e86defeb87a6b44dc8eb53f91fbea3d1a87"
}

search = GoogleSearch(params)
results = search.get_dict()
events_results = results["events_results"]

print(events_results)
