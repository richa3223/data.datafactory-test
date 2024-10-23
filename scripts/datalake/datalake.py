from azure.graphrbac import GraphRbacManagementClient
from azure.cli.core import AzCli
from azure.cli.core._profile import Profile
from azure.core.exceptions import ResourceExistsError
from azure.multiapi.storagev2.filedatalake.v2020_02_10 import (
    DataLakeDirectoryClient,
    DataLakeServiceClient,
)
from azure.identity import AzureCliCredential
from dotenv import load_dotenv
import traceback
import argparse
import json
import uuid
import sys
import os


global identities
identities = {}


def get_graphrbac_client():
    cli_ctx = AzCli(
        config_dir=os.path.expanduser(
            os.environ.get("AZURE_CONFIG_DIR", "~/.azure")
        )
    )
    profile = Profile(cli_ctx=cli_ctx)
    cred, _, tenant_id = profile.get_login_credentials(
        resource=cli_ctx.cloud.endpoints.active_directory_graph_resource_id
    )
    return GraphRbacManagementClient(
        cred,
        tenant_id,
        cli_ctx.cloud.endpoints.active_directory_graph_resource_id,
    )


def try_parse_uuid(uuid_string):
    try:
        return uuid.UUID(uuid_string)
    except:
        return None


def get_identity_uuid(graph, identity, group=None):
    if identity is None or len(identity) == 0:
        return identity

    identity_uuid = try_parse_uuid(identity)
    if identity_uuid is None:
        identity_uuid = identities.get(identity)
        if identity_uuid is None:
            filter = f"displayName eq '{identity}'"
            response = (
                [g.object_id for g in graph.groups.list(filter=filter)]
                if group
                else [u.object_id for u in graph.users.list(filter=filter)]
            )

            if len(response) == 1:
                identity_uuid = response[0]
                identities[identity] = identity_uuid
            else:
                raise Exception(
                    f"Unable to resolve identity: {group}:{identity}"
                )

    return str(identity_uuid)


def build_ace(graph, ace_dict):
    ace_is_default = ace_dict.get("default", False)
    ace_type = ace_dict.get("type")
    ace_identity = get_identity_uuid(
        graph, ace_dict.get("identity", ""), ace_type.endswith("group")
    )
    ace_permission = ace_dict.get("permission", "---")
    return (
        f"default:{ace_type}:{ace_identity}:{ace_permission}"
        if ace_is_default
        else f"{ace_type}:{ace_identity}:{ace_permission}"
    )


def set_acl(
    credential,
    account_name,
    file_system,
    path,
    owner,
    group,
    acl,
    recursive,
    metadata=None,
):
    graph = get_graphrbac_client()

    owner_uuid = get_identity_uuid(graph, owner, False)
    group_uuid = get_identity_uuid(graph, group, True)

    if isinstance(acl, list):
        acl = str.join(",", [build_ace(graph, x) for x in acl])

    client = DataLakeDirectoryClient(
        f"https://{account_name}.dfs.core.windows.net/",
        file_system_name=file_system,
        directory_name=path,
        credential=credential,
    )
    if path != "/":
        result = client.create_directory()
        print(result)

    if recursive:
        if owner_uuid is not None or group_uuid is not None:
            result = client.set_access_control(owner=owner, group=group)
            print(result)

        result = client.set_access_control_recursive(acl)
        print(result)
    else:
        result = client.set_access_control(
            owner=owner_uuid, group=group_uuid, acl=acl
        )
        print(result)


def process_input_file_paths(
    env, credential, account, file_system, paths, acls
):
    metadata = paths.pop("$metadata") if "$metadata" in paths.keys() else None

    for k, v in env.items():
        account = account.replace(k, v)
        file_system = file_system.replace(k, v)

        if metadata is not None:
            meta = {}
            for mk in metadata.keys():
                mv = metadata.pop(mk)
                mk = mk.replace(k, v)
                mv = mv.replace(k, v)
                meta[mk] = mv
            metadata = meta

    service = DataLakeServiceClient(
        f"https://{account}.dfs.core.windows.net/", credential=credential
    )

    fs = service.get_file_system_client(file_system)
    try:
        result = fs.create_file_system(metadata=metadata)
        print(result)
    except ResourceExistsError as e:
        pass

    for path, data in paths.items():
        owner = data.get("owner")
        group = data.get("group")
        acl = data.get("acl")
        recursive = data.get("recursive", True)
        metadata = data.get("$metadata")

        for k, v in env.items():
            if owner is not None:
                owner = owner.replace(k, v)
            if group is not None:
                group = group.replace(k, v)
            if metadata is not None:
                meta = {}
                for mk in metadata.keys():
                    mv = metadata.pop(mk)
                    mk = mk.replace(k, v)
                    mv = mv.replace(k, v)
                    meta[mk] = mv
                metadata = meta

        if isinstance(acl, str) and acl.startswith("#"):
            acl = acls.get(acl[1:], acl)

        if isinstance(acl, str):
            for k, v in env.items():
                acl = acl.replace(k, v)
        elif isinstance(acl, list):
            for a in acl:
                identity = a.get("identity")
                if identity is not None:
                    for k, v in env.items():
                        identity = identity.replace(k, v)
                    a["identity"] = identity

        set_acl(
            credential,
            account,
            file_system,
            path,
            owner,
            group,
            acl,
            recursive,
            metadata,
        )


def process_input_file(args, env, credential):
    with open(args.input_file, "r") as f:
        settings = json.load(f)

    acls = settings.pop("$acls") if "$acls" in settings.keys() else {}

    if "accounts" in settings.keys():
        for account, file_systems in settings.pop("accounts").items():
            for file_system, paths in file_systems.items():
                process_input_file_paths(
                    env, credential, account, file_system, paths, acls
                )

    if "file_systems" in settings.keys():
        for file_system, paths in settings.pop("file_systems").items():
            process_input_file_paths(
                env, credential, args.account_name, file_system, paths, acls
            )

    if len(settings) > 0:
        process_input_file_paths(
            env,
            credential,
            args.account_name,
            args.file_system,
            settings.items(),
            acls,
        )


def process_command_line(args, env, credential):
    for k, v in env.items():
        if args.account is not None:
            args.account = args.account.replace(k, v)
        if args.file_system is not None:
            args.file_system = args.file_system.replace(k, v)
        if args.path is not None:
            args.path = args.path.replace(k, v)
        if args.acl is not None:
            args.acl = args.acl.replace(k, v)
    set_acl(
        credential,
        args.account_name,
        args.file_system,
        args.path,
        None,
        None,
        args.acl,
        True,
    )


def main(args, env):
    credential = (
        AzureCliCredential() if args.storage_key is None else args.storage_key
    )

    if args.input_file is not None:
        process_input_file(args, env, credential)
    else:
        process_command_line(args, env, credential)


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--account-name", type=str, default=os.environ.get("ACCOUNT_NAME")
    )
    parser.add_argument(
        "--file-system", type=str, default=os.environ.get("FILE_SYSTEM")
    )
    parser.add_argument(
        "--path", type=str, default=os.environ.get("PATH", "/")
    )
    parser.add_argument(
        "--acl",
        type=str,
        default=os.environ.get("ACL", "user::rwx,group::r-x,other::---"),
    )
    parser.add_argument(
        "--input-file", type=str, default=os.environ.get("INPUT_FILE")
    )
    parser.add_argument(
        "--storage-key", type=str, default=os.environ.get("STORAGE_KEY")
    )
    parsed = parser.parse_known_args()

    try:
        args = parsed[0]
        env = {"$" + p[0]: p[1] for p in [x.split("=", 2) for x in parsed[1]]}
        env.update(
            {
                "$" + k[13:]: v
                for k, v in os.environ.items()
                if k.startswith("DATALAKE_ENV_")
            }
        )
        print(args)
        print(env)
        main(args, env)
        exit(0)
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(f"exc_type={exc_type}")
        print(f"exc_value={exc_value}")
        print(f"exc_traceback={exc_traceback}")
        traceback.print_tb(exc_traceback)
        print(str(e))
        exit(1)
