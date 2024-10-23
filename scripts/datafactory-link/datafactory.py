from azure.identity import AzureCliCredential
from dotenv import load_dotenv
import requests
import argparse
import os


def main(args):
    cred = AzureCliCredential()
    token = cred.get_token("https://management.azure.com/").token
    headers = {"Authorization": f"Bearer {token}"}
    url = (
        "https://management.azure.com"
        f"/subscriptions/{args.subscription}"
        f"/resourceGroups/{args.data_factory_rg}"
        "/providers/Microsoft.DataFactory"
        f"/factories/{args.data_factory_name}"
        f"/managedVirtualNetworks/{args.data_factory_vnet}"
        "/managedPrivateEndpoints"
        "?api-version=2018-06-01"
    )
    response = requests.get(url, headers=headers)
    endpoints = response.json()
    print(endpoints)
    links = []
    for endpoint in endpoints.get("value", []):
        properties = endpoint.get("properties", {})
        group = properties.get("groupId")
        state = properties.get("connectionState", {}).get("status")
        if state and state == "Pending":
            links.append(
                (
                    properties.get("connectionState", {}).get("description"),
                    properties.get("privateLinkResourceId"),
                    group,
                )
            )

    if len(links) == 0:
        print("No private links require approval")

    api_versions = {"blob": "2021-08-01", "dfs": "2021-08-01"}

    for desc, id, group in links:
        print(f"Processing {desc}")
        url = (
            f"https://management.azure.com/{id}"
            "/privateEndpointConnections"
            "?api-version=2021-08-01"
        )
        response = requests.get(url, headers=headers)
        connections = response.json()
        print(connections)
        for connection in connections.get("value", []):
            properties = connection.get("properties", {}).get(
                "privateLinkServiceConnectionState", {}
            )
            state = properties.get("status")
            description = properties.get("description")
            if state and state == "Pending" and description == desc:
                link_id = connection.get("id")
                url = (
                    f"https://management.azure.com/{link_id}"
                    f"?api-version={api_versions[group]}"
                )
                print(properties)
                data = {
                    "properties": {
                        "privateLinkServiceConnectionState": {
                            "status": "Approved",
                            "description": description,
                        }
                    },
                    "privateEndpoint": {"id": id},
                }
                response = requests.put(url, headers=headers, json=data)
                print(response.json())
                print("Successfully approved")
            else:
                print(f"No approval required: {connection}")


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--subscription", type=str, default=os.environ.get("SUBSCRIPTION_ID")
    )
    parser.add_argument(
        "--data-factory-rg",
        type=str,
        default=os.environ.get("DATAFACTORY_RESOURCE_GROUP"),
    )
    parser.add_argument(
        "--data-factory-name",
        type=str,
        default=os.environ.get("DATAFACTORY_NAME"),
    )
    parser.add_argument(
        "--data-factory-vnet",
        type=str,
        default=os.environ.get("DATAFACTORY_VNET", "default"),
    )

    args = parser.parse_args()
    try:
        main(args)
        exit(0)
    except Exception as e:
        print(str(e))
        exit(1)
