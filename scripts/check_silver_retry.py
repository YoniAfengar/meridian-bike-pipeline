import json
import subprocess
import time


market = "jc"
month = "2026-06"

pipeline = subprocess.Popen(
    ["just", "run", "pipeline", market, month],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
)

deadline = time.monotonic() + 120
killed = None

while pipeline.poll() is None and time.monotonic() < deadline:
    listing = subprocess.check_output(
        [
            "docker", "ps",
            "--filter",
            "label=com.docker.compose.project=meridian-bike-pipeline",
            "--filter",
            "label=com.docker.compose.service=app",
            "--format", "{{.ID}}",
        ],
        text=True,
    )

    for container in listing.split():
        inspected = subprocess.run(
            [
                "docker", "inspect",
                "--format", "{{json .Config.Cmd}}",
                container,
            ],
            capture_output=True,
            text=True,
        )
        if inspected.returncode:
            continue

        command = json.loads(inspected.stdout)
        if command == [
            "run", "transform-to-silver", f"trips:{market}", month
        ]:
            result = subprocess.run(
                ["docker", "kill", container],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                killed = container
                break

    if killed:
        break

    time.sleep(0.2)

stdout, stderr = pipeline.communicate(timeout=600)

if killed is None:
    raise SystemExit(
        f"FAIL: no silver container was killed\n{stdout}\n{stderr}"
    )

if pipeline.returncode:
    raise SystemExit(f"FAIL: pipeline failed\n{stdout}\n{stderr}")

run = json.loads(stdout)
silver = run["tasks"][f"transform-to-silver trips:{market} {month}"]

if run["state"] != "success" or silver["tries"] < 2:
    raise SystemExit(f"FAIL: retry was not verified\n{stdout}")

print(json.dumps({
    "test": "silver_container_retry",
    "passed": True,
    "killed_container": killed,
    "silver_tries": silver["tries"],
    "run_state": run["state"],
}))
