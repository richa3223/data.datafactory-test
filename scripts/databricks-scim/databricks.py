from azure.identity import AzureCliCredential
from dotenv import load_dotenv
import requests
import argparse
import os


def add_service_principal(headers, args):
    url = f"https://{args.workspace_url}/api/2.0/preview/scim/v2/ServicePrincipals"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("Failed to query service principals in Databricks")
        exit(1)

    service_principals = [
        x
        for x in response.json().get("Resources", [])
        if x.get("applicationId") == args.service_principal
    ]
    if service_principals:
        print("Service principal already found, terminating")
        exit(0)
    else:
        print("Service principal does not exist, it will be created")

        url = f"https://{args.workspace_url}/api/2.0/preview/scim/v2/Groups"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print("Failed to query groups in Databricks")
            exit(1)

        groups = [
            {"value": x.get("id")}
            for x in response.json().get("Resources", [])
            if x.get("displayName") in args.groups
        ]

        data = {
            "schemas": [
                "urn:ietf:params:scim:schema:core:2.0:ServicePrincipal"
            ],
            "applicationId": args.service_principal,
            "displayName": args.service_principal_name,
            "groups": groups,
            "entitlements": [{"value": "allow-cluster-create"}],
        }

        url = f"https://{args.workspace_url}/api/2.0/preview/scim/v2/ServicePrincipals"
        response = requests.post(url, headers=headers, json=data)
        print(response)
        print(response.json())


def main(args):
    cred = AzureCliCredential()
    token = cred.get_token(
        "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
    ).token
    headers = {"Authorization": f"Bearer {token}"}
    args.entry(headers, args)


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser()

    commands = parser.add_subparsers(help="Commands")
    sp_commands = commands.add_parser("sp")
    sp_commands.set_defaults(entry=add_service_principal)
    sp_commands.add_argument("--workspace-url", type=str, default=os.environ.get("WORKSPACE_URL"))
    sp_commands.add_argument("--service-principal", type=str, default=os.environ.get("SERVICE_PRINCIPAL"))
    sp_commands.add_argument("--service-principal-name", type=str, default=os.environ.get("SERVICE_PRINCIPAL_NAME"))
    sp_commands.add_argument("--groups", type=list, nargs="+", default=["admins"])

    parsed = parser.parse_known_args()
    main(parsed[0])
