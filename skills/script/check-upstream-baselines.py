#!/usr/bin/env python3
# SPDX-License-Identifier: MIT

import argparse
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def fail(message: str) -> None:
    raise SystemExit(message)


def git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def require_revision(checkout: Path, expected: str, label: str) -> None:
    checkout = checkout.resolve()
    if not (checkout / ".git").exists():
        fail(f"{label} is not a Git checkout: {checkout}")
    actual = git(checkout, "rev-parse", "HEAD")
    resolved = git(checkout, "rev-parse", f"{expected}^{{commit}}")
    if actual != resolved:
        fail(f"{label} checkout {actual} does not match baseline {resolved}")


def require_text(path: Path, *needles: str) -> None:
    content = path.read_text()
    for needle in needles:
        if needle not in content:
            fail(f"{path} is missing required text: {needle}")


def require_ancestor(checkout: Path, ancestor: str, descendant: str, label: str) -> None:
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=checkout,
        check=False,
    )
    if result.returncode != 0:
        fail(f"{label} {ancestor} is not an ancestor of {descendant}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dex-root", required=True, type=Path)
    arguments = parser.parse_args()

    server_baseline = (ROOT / "DEX_SERVER_BASELINE").read_text().strip()
    cli_baseline = (ROOT / "DEX_CLI_BASELINE").read_text().strip()
    require_revision(arguments.dex_root, cli_baseline, "Dex CLI")
    require_ancestor(arguments.dex_root, server_baseline, cli_baseline, "Dex Server baseline")

    require_text(
        arguments.dex_root / "cli" / "internal" / "flowviz" / "v2_directives.go",
        "GetDexSummary",
        "GetDexDisplay",
        "dex:group",
        "RPCOptions.Action",
        "ActionRequiresPermission",
        '"ui-slot"',
        '"capture"',
        '"qr-code"',
        "capture requires a source:user string input field",
    )
    require_text(
        arguments.dex_root / "cli" / "schema" / "flow-definition-graph.v2.schema.json",
        '"capture"',
        '"qr-code"',
    )
    require_text(
        arguments.dex_root / "cli" / "README.md",
        "--schema-version 2.0",
        "Go-only",
    )
    require_text(
        arguments.dex_root / "web" / "api" / "v2.go",
        "V2DefinitionRevisionHeader",
        'json:"workQueuePermissions"',
        "Action permission denied",
        'json:"capture,omitempty"',
    )
    require_text(
        arguments.dex_root / "web" / "app" / "v2" / "workspace" / "qrScanner.ts",
        "await import('@zxing/browser')",
        "facingMode: { ideal: 'environment' }",
        "for (const track of stream.getTracks()) track.stop()",
    )
    require_text(
        arguments.dex_root / "sdk-go" / "README.md",
        "Once a permission has",
        "remains searchable after completion",
    )
    require_text(
        arguments.dex_root / "cli" / "internal" / "flowviz" / "go_connector_factory.go",
        "parseConnectorFactoryStep",
        "parseConnectorAnnotations",
        r"(?:-rc\.[0-9]+)?",
        '"annotations"',
        '"connectionName"',
        '"moduleVersion"',
    )
    require_text(
        arguments.dex_root / "cli" / "internal" / "flowviz" / "go_connector_trigger.go",
        "collectConnectorTriggerBindings",
        '"bindingName"',
        "TriggerBindingFactoryConfigMarker",
    )
    require_text(
        arguments.dex_root / "cli" / "internal" / "dev" / "config.go",
        "connector-config-dir",
        "connector-release-override",
    )
    require_text(
        arguments.dex_root / "web" / "connector_connection_store.go",
        "connections.json",
        "TriggerBindings",
        "putTriggerBinding",
        "os.Rename",
    )
    require_text(
        arguments.dex_root / "web" / "connector_oauth.go",
        "code_verifier",
        "connectorOAuthResponseValue",
        "credentials[mapping.Credential]",
        "credentialSecrets",
    )
    require_text(
        arguments.dex_root / "cli" / "internal" / "flowviz" / "go_connector_configuration_ui.go",
        "ConnectorConfigurationUI",
        "ConnectorUIUnit",
        "ConnectorUIBinding",
        "compile-time literals",
    )
    require_text(
        arguments.dex_root / "web" / "connector_studio_command.go",
        "connectorStudioCommandRequest",
        "CONNECTOR_STUDIO_COMMAND_NOT_FOUND",
        "containsConnectorSecret",
        "https",
    )
    require_text(
        arguments.dex_root / "web" / "connector_use_configuration.go",
        "validateConnectorUseConfiguration",
        "allowedPointers",
        "Connector configuration path",
    )
    require_text(
        arguments.dex_root / "web" / "app" / "v2" / "connectors" / "ConnectorsPage.tsx",
        "provider.command.execute",
        "use.configuration.save",
        "connector.frame.resize",
        "Local override",
    )
    require_text(
        arguments.dex_root / "web" / "api" / "v2_start.go",
        "WORKER_UNHEALTHY",
        "BypassWorkerHealthCheck",
        "v2StartStepInput",
        "SkipWaitFor: definition.Start.SkipWaitFor",
        "requestID := uuid.NewString()",
    )
    require_text(
        arguments.dex_root / "web" / "flow_definition_provider.go",
        "filepath.WalkDir",
        "FLOW_DEFINITION_INVALID",
        "FLOW_DEFINITION_SOURCE_UNAVAILABLE",
    )
    require_text(
        arguments.dex_root / "server" / "config" / "config.go",
        'yaml:"flowRenderingDirectory"',
        'yaml:"startFlowWorkerTargetHeadless"',
        'yaml:"rowKey"',
        'yaml:"columnNaming"',
        'yaml:"attributeMaps"',
        'yaml:"instanceKeyColumn"',
    )
    require_text(
        arguments.dex_root / "server" / "service" / "common" / "attributestore" / "sql_store.go",
        "confirmedMissingColumns",
        "needsColumnRefresh",
        "refresh Attribute Store schema for an unknown column",
    )
    require_text(
        arguments.dex_root / "web" / "connector_release_oauth_settings.go",
        "fromSetting",
    )
    require_text(
        arguments.dex_root / "web" / "connector_oauth.go",
        "CONNECTOR_OAUTH_SETTING_INVALID",
        "client_secret_basic",
    )
    require_text(
        arguments.dex_root / "web" / "connector_address_policy.go",
        "IsLinkLocalUnicast",
        "IsPrivate",
        "connectorAWSIPv6MetadataAddress",
    )
    require_text(
        arguments.dex_root / "cli" / "internal" / "flowviz" / "go_state_loads.go",
        "attribute_map_read_not_loaded",
        "attribute_map_enumeration_not_loaded",
        "attribute_map_instance_not_loaded",
        "channel_messages_not_loaded",
    )
    require_text(
        arguments.dex_root / "cli" / "internal" / "flowviz" / "go_analyzer.go",
        "step_progress_outside_step",
    )
    require_text(
        arguments.dex_root / "cli" / "internal" / "flowviz" / "go_start_flow_inputs.go",
        "start_flow_input_type_mismatch",
    )
    print("validated Dex Server and CLI baselines")


if __name__ == "__main__":
    main()
