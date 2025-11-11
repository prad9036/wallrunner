import json

file = "wallpapers.json"
with open(file) as f:
    data = json.load(f)

if isinstance(data, list) and all(isinstance(x, dict) for x in data):
    print("✅ Ready for mongoimport --jsonArray")
elif all(line.strip().startswith("{") and line.strip().endswith("}") for line in open(file)):
    print("✅ Ready for mongoimport (one document per line)")
elif isinstance(data, dict) and "wallpapers" in data:
    print("⚠️ Has nested array; extract data['wallpapers'] before importing")
else:
    print("❌ Not compatible with mongoimport structure")
