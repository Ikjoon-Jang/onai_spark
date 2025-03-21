import requests

def upload_to_fuseki(rdf_data):
    fuseki_url = "http://3.36.178.68:3030/dataset/update"
    headers = {"Content-Type": "application/sparql-update"}

    sparql_update = f"INSERT DATA {{ {rdf_data} }}"
    response = requests.post(fuseki_url, data=sparql_update, headers=headers)

    if response.status_code == 200:
        print("✅ 성공적으로 업로드됨!")
    else:
        print(f"❌ 오류: {response.status_code} - {response.text}")