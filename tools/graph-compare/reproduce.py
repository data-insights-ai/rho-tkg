#!/usr/bin/env python3
"""Reproduce the accepted native-v4 current-answer slice in an owned workspace."""
import argparse
from contextlib import contextmanager
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parent
SOURCE_SHA = "7336760a9be3bc3885c08307cf5aad05181e602d06b35d22bb298213c7580476"
REFERENCE_SHA = "771f4854792d49fd2619327a2c1544196be62c91d18ea5d75a25a1b591e6b7f5"
COMMIT = "4126bb108cd3fae2f0818052d08a31f0b5466c6e"
TREE = "4c2ca34b38cd7ec394dfa4c83ea18350197400b2"
GRAPH_MODULE = "github.com/data-insights-ai/rho-tkg/v4"
CHECKS = ("build", "normal", "race", "vet", "cover", "fmt", "lint", "security", "vulncheck", "exports")


class Refusal(RuntimeError):
    """No success evidence was published."""


def digest(data):
    return hashlib.sha256(data).hexdigest()


def validate_environment(ambient):
    for key in ("GOFLAGS", "GOOS", "GOARCH", "GOEXPERIMENT", "GOROOT"):
        if ambient.get(key):
            raise Refusal("ambient build override: " + key)
    if ambient.get("GOWORK", "off") not in ("", "off"):
        raise Refusal("ambient workspace override")
    if ambient.get("GOTOOLCHAIN", "local") not in ("", "local"):
        raise Refusal("ambient toolchain override")


def compiler(binary, ambient, online=False):
    validate_environment(ambient)
    resolved = shutil.which(binary) if not Path(binary).is_absolute() else binary
    if not resolved or not Path(resolved).is_file():
        raise Refusal("explicit Go1.26.9 compiler unavailable")
    resolved = str(Path(resolved).resolve())
    env = dict(ambient)
    env.update(GOTOOLCHAIN="local", GOWORK="off", GOENV="off", GOFLAGS="-mod=readonly",
               GOPROXY="https://proxy.golang.org,direct" if online else "off",
               GOSUMDB="sum.golang.org" if online else "off")
    version = checked([resolved, "version"], ROOT, env).decode().strip()
    if not version.startswith("go version go1.26.9 "):
        raise Refusal("requires explicit Go1.26.9 compiler")
    details = json.loads(checked([resolved, "env", "-json", "GOHOSTOS", "GOHOSTARCH", "GOOS", "GOARCH", "GOVERSION", "CGO_ENABLED", "GOAMD64", "GOARM64"], ROOT, env))
    native_os = {"Darwin": "darwin", "Linux": "linux"}.get(platform.system())
    native_arch = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64", "AMD64": "amd64"}.get(platform.machine())
    if (details["GOOS"], details["GOARCH"]) != (native_os, native_arch) or (details["GOHOSTOS"], details["GOHOSTARCH"]) != (native_os, native_arch):
        raise Refusal("requires a matched native compiler; cross compilation refused")
    return resolved, env, {"version": version, "binary_sha256": digest(Path(resolved).read_bytes()),
                           "native_os": native_os, "native_arch": native_arch, "effective_build_environment": details,
                           "environment": {key: env[key] for key in ("GOTOOLCHAIN", "GOWORK", "GOENV", "GOFLAGS", "GOPROXY", "GOSUMDB")}}


def checked(command, cwd, env=None):
    try:
        with subprocess.Popen(command, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True) as process:
            try:
                stdout, stderr = process.communicate(timeout=300)
            except BaseException:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise
            if process.returncode:
                raise Refusal("command refused; exit=%s output_sha256=%s" % (process.returncode, digest(stdout + stderr)))
            return stdout
    except (OSError, subprocess.TimeoutExpired) as err:
        raise Refusal("command unavailable or timed out") from err


def hash_tree(root):
    if root.is_symlink() or not root.is_dir():
        raise Refusal("invalid source directory")
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise Refusal("symlink source refused")
        if path.is_dir():
            continue
        if not path.is_file():
            raise Refusal("nonregular source refused")
        result[str(path.relative_to(root))] = digest(path.read_bytes())
    return result


def content_sha(files):
    return digest(json.dumps(files, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode() + b"\n")


def load_locks(tool):
    result = []
    for name, expected in (("source.json", SOURCE_SHA), ("reference.json", REFERENCE_SHA)):
        path = tool / "pins" / name
        try:
            data = path.read_bytes()
            if digest(data) != expected:
                raise Refusal("trusted lock mismatch")
            result.append(json.loads(data))
        except (OSError, ValueError) as err:
            raise Refusal("trusted lock unavailable") from err
    return tuple(result)


def verify_release(root, lock):
    if lock["commit"] != COMMIT or lock["tree"] != TREE or lock["file_count"] != 1438:
        raise Refusal("wrong release identity")
    files = hash_tree(root)
    if len(files) != lock["file_count"] or content_sha(files) != lock["content_inventory_sha256"]:
        raise Refusal("release content mismatch")
    return files


def extract_release(repository, target, lock):
    commit = checked(["git", "rev-parse", lock["commit"] + "^{commit}"], repository).decode().strip()
    tree = checked(["git", "rev-parse", lock["commit"] + "^{tree}"], repository).decode().strip()
    if commit != COMMIT or tree != TREE or commit != lock["commit"] or tree != lock["tree"]:
        raise Refusal("actual Git identity mismatch")
    archive = checked(["git", "archive", "--format=tar", COMMIT], repository)
    target.mkdir(mode=0o700)
    try:
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as members:
            seen = set()
            for member in members:
                name = Path(member.name)
                if name.is_absolute() or ".." in name.parts or str(name) != member.name or member.name in seen:
                    raise Refusal("unsafe or duplicate archive path")
                seen.add(member.name)
                destination = target / name
                if member.isdir():
                    destination.mkdir(parents=True, exist_ok=True)
                elif member.isfile():
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(members.extractfile(member).read())
                    destination.chmod(member.mode & 0o777)
                else:
                    raise Refusal("nonregular archive member")
        files = verify_release(target, lock)
    except BaseException:
        shutil.rmtree(target)
        raise
    return {"commit": commit, "tree": tree, "file_count": len(files),
            "content_inventory_sha256": content_sha(files), "observed_archive_sha256": digest(archive),
            "historical_source_manifest_sha256": lock["historical_source_manifest_sha256"]}


def verify_reference(root, pin):
    if hash_tree(root) != pin["files_sha256"]:
        raise Refusal("reference content mismatch")


def copy_reference(source, target, pin):
    verify_reference(source, pin)
    shutil.copytree(source, target)
    verify_reference(target, pin)


@contextmanager
def owned_workspace(output=None):
    """Stage privately; publish with exclusive mkdir and completion manifest last."""
    output = Path(output) if output is not None else None
    if output is not None:
        if os.path.lexists(output) or not output.parent.is_dir():
            raise Refusal("destination exists or parent unavailable")
    with tempfile.TemporaryDirectory(prefix="rho-compare-", dir=output.parent if output is not None else None) as temporary:
        staged = Path(temporary)
        yield staged
        if output is not None:
            try:
                output.mkdir(mode=0o700)
            except OSError as err:
                raise Refusal("destination no longer available") from err
            identity = output.stat()
            try:
                # Completed outputs are staged before this point; success manifest moves last.
                for path in sorted(staged.iterdir(), key=lambda item: item.name == "completion.json"):
                    shutil.move(str(path), output / path.name)
            except BaseException:
                current = output.stat()
                if (current.st_dev, current.st_ino) == (identity.st_dev, identity.st_ino):
                    shutil.rmtree(output)
                raise


def portable_receipt(command, executable_role, exit_code, output):
    return {"command": [executable_role, *["{absolute-input}" if Path(arg).is_absolute() else arg for arg in command[1:]]], "exit_code": exit_code,
            "output_sha256": digest(output)}


def run_checks(repository, tool, binary, checks, output=None, online=False, docker_scanners=False):
    go, env, provenance = compiler(binary, os.environ, online)
    source, reference = load_locks(tool)
    if not checks or any(name not in CHECKS for name in checks) or len(set(checks)) != len(checks):
        raise Refusal("unknown or duplicate checks")
    with owned_workspace(output) as publication:
        work = publication / "work"
        work.mkdir()
        source_receipt = extract_release(repository, work / "rho-v4-source", source)
        copy_reference(repository / "docs/v5/reference/independent/comparison-inputs", work / "reference", reference)
        module = work / "harness"
        shutil.copytree(tool, module, ignore=shutil.ignore_patterns("artifacts", "__pycache__", "coverage*"))
        shutil.copy2(tool / "pins/source.json", work / "source-pin.json")
        shutil.copy2(tool / "pins/reference.json", work / "reference-pin.json")
        mod = module / "go.mod"
        if "replace " in mod.read_text() or "go 1.26.9" not in mod.read_text() or GRAPH_MODULE + " v4.43.0" not in mod.read_text():
            raise Refusal("canonical module or source replacement mismatch")
        mod.write_text(mod.read_text() + "\nreplace " + GRAPH_MODULE + " => ../rho-v4-source\n")
        sums = digest((module / "go.sum").read_bytes())
        receipts = []

        def invoke(args, cwd=module, role="go", executable=go):
            command = [executable, *args]
            result = checked(command, cwd, env)
            receipts.append({"cwd": "harness" if cwd == module else "work", **portable_receipt(command, role, 0, result)})
            return result

        invoke(["mod", "download"])
        invoke(["mod", "verify"])
        if sums != digest((module / "go.sum").read_bytes()):
            raise Refusal("dependency checksum file changed")
        dep = json.loads(invoke(["list", "-m", "-json", GRAPH_MODULE]))
        if dep["Version"] != "v4.43.0" or Path(dep["Dir"]).resolve() != (work / "rho-v4-source").resolve() or dep.get("Replace", {}).get("Path") != "../rho-v4-source":
            raise Refusal("compiler dependency is not the pinned extracted engine")
        build_mod = digest(mod.read_bytes())
        compiled_inputs = hash_tree(module)
        scanner_evidence = {}
        exports_inventory = {}
        scanner_specs = {"lint": ("golangci-lint", "github.com/golangci/golangci-lint/v2", "v2.13.2"), "security": ("gosec", "github.com/securego/gosec/v2", "v2.29.0"), "vulncheck": ("govulncheck", "golang.org/x/vuln", "v1.7.0")}
        docker_base = None
        if docker_scanners and any(check in scanner_specs for check in checks):
            image = json.loads(checked(["docker", "image", "inspect", "golang:1.26.9"], module))[0]
            mod_cache = invoke(["env", "GOMODCACHE"]).decode().strip()
            shutil.copy2(repository / ".golangci.yml", work / ".golangci.yml")
            docker_base = ["docker", "run", "--rm", "--pull=never", "-e", "GOTOOLCHAIN=local", "-e", "GOWORK=off", "-e", "GOFLAGS=-mod=readonly", "-e", "GOPROXY=off", "-e", "GOSUMDB=off", "-v", str(work)+":/src:ro", "-w", "/src/harness", "-v", "rho-tkg-gocache:/go", "-v", mod_cache+":/go/pkg/mod:ro", "-v", "rho-tkg-buildcache:/root/.cache/go-build", image["Id"]]
            version = checked([*docker_base, "go", "version"], module).decode().strip()
            if not version.startswith("go version go1.26.9 linux/"):
                raise Refusal("Docker scanner compiler mismatch")
            checked([*docker_base, "go", "mod", "verify"], module)
            scanner_evidence["container"] = {"image_id": image["Id"], "os": image["Os"], "architecture": image["Architecture"], "compiler": version, "scope": "Static/security tooling in cached Linux container; no hardware/performance equivalence"}

        def scan(check):
            name, path, version = scanner_specs[check]
            if docker_base:
                executable = "/go/bin/" + name
                info = checked([*docker_base, "go", "version", "-m", executable], module)
            else:
                executable = shutil.which(name)
                if not executable:
                    raise Refusal("pinned scanner unavailable; provision explicitly or use cached Docker mode")
                info = invoke(["version", "-m", executable])
            if ("\tmod\t" + path + "\t" + version + "\t").encode() not in info:
                raise Refusal("scanner module/version mismatch")
            scanner_evidence[name] = {"module": path, "version": version, "buildinfo_sha256": digest(info)}
            arguments = {"lint": ["run", "--config", "../.golangci.yml" if docker_base else str(repository / ".golangci.yml"), "./..."], "security": ["-quiet", "./..."], "vulncheck": ["./..."]}[check]
            if docker_base:
                result = checked([*docker_base, executable, *arguments], module)
                receipts.append({"cwd": "harness", "command": [name, *arguments], "exit_code": 0, "output_sha256": digest(result)})
            else:
                invoke(arguments, executable=executable, role=name)

        commands = {"build": ["build", "-trimpath", "-buildvcs=false", "./..."],
                    "normal": ["test", "-trimpath", "-buildvcs=false", "-count=1", "-timeout=240s", "./..."],
                    "race": ["test", "-trimpath", "-buildvcs=false", "-race", "-count=1", "-timeout=240s", "./..."],
                    "vet": ["vet", "./..."],
                    "cover": ["test", "-trimpath", "-buildvcs=false", "-count=1", "-timeout=240s", "-coverprofile=../coverage.out", "./..."]}
        for check in checks:
            if check in commands:
                invoke(commands[check])
            elif check == "fmt":
                result = invoke(["-l", *[str(f.relative_to(module)) for f in sorted(module.rglob("*.go"))]], executable=str(Path(go).with_name("gofmt")), role="gofmt")
                if result:
                    raise Refusal("unformatted tool source")
            elif check in scanner_specs:
                scan(check)
            elif check == "exports":
                invoke(["build", "-trimpath", "-buildvcs=false", "-o", "../graph-compare", "./cmd/graph-compare"])
                for backend in ("memory", "badger"):
                    invoke(["run", "--backend", backend, "--out", "../" + backend], executable=str(work / "graph-compare"), role="graph-compare")
                    native = json.loads((work / backend / "report.json").read_text())
                    expected_reopen = 23 if backend == "badger" else 0
                    if (native["validated_current_answers"],native["pending_history_answers"],native["validated_r3_reopen_answers"],native["rho_source_commit"],native["backend"],native["performance_acceptance"],native["native_v5"]) != (92,1,expected_reopen,COMMIT,backend,False,False):
                        raise Refusal("complete native export inventory mismatch")
                    exports_inventory[backend] = {"current":92,"pending_history":1,"reopened_current":expected_reopen}
                    shutil.move(str(work / backend), publication / backend)
                (publication / "binary.sha256").write_text(digest((work / "graph-compare").read_bytes()) + "\n")
                (publication / "binary-buildinfo.sha256").write_text(digest(invoke(["version", "-m", "../graph-compare"])) + "\n")
        if "cover" in checks:
            detail = invoke(["tool", "cover", "-func=../coverage.out"]).decode()
            import re
            total = re.search(r"total:.*?([0-9.]+)%", detail)
            if not total or float(total[1]) < 80:
                raise Refusal("missing or below80 tool coverage")
            shutil.copy2(work / "coverage.out", publication / "coverage.out")
            (publication / "coverage-functions.txt").write_text(detail)
        verify_release(work / "rho-v4-source", source)
        verify_reference(work / "reference", reference)
        if hash_tree(module) != compiled_inputs or sums != digest((module / "go.sum").read_bytes()) or build_mod != digest(mod.read_bytes()):
            raise Refusal("module/dependency inputs changed during checks")
        report = {"schema_version": 1, "status": "passed-selected-native-v4-tool-checks",
                  "checks": checks, "compiler": provenance, "scanners": scanner_evidence, "exports_inventory": exports_inventory, "source": source_receipt,
                  "harness_inputs_sha256": compiled_inputs,
                  "source_pin_sha256": SOURCE_SHA, "reference_pin_sha256": REFERENCE_SHA,
                  "dependency_verification": "go mod download +go mod verify; readonly builds; unchanged sums and validated relative source replacement",
                  "receipts": receipts, "performance_acceptance": False, "native_v5": False,
                  "evidence_limits": ["Completion manifest published last; no crash-atomic whole-directory or power-loss publication guarantee",
                                      "Private serial runner; MaxVisited limits delivered callbacks only, not native internal work/memory",
                                      "No vendors, services, benchmarks or complete phase acceptance"]}
        shutil.rmtree(work)
        (publication / "completion.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def format_tool(binary):
    go,env,_=compiler(binary,os.environ)
    checked([str(Path(go).with_name("gofmt")),"-w",*[str(f) for f in sorted(ROOT.rglob("*.go"))]],ROOT,env)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", default="go", help="explicit native Go1.26.9 executable")
    parser.add_argument("--repository", type=Path, default=ROOT.parents[1])
    parser.add_argument("--checks", default="build,normal,race,vet,cover,fmt,exports")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--allow-dependency-download", action="store_true")
    parser.add_argument("--format-tool", action="store_true")
    parser.add_argument("--docker-scanners", action="store_true", help="use already cached Go1.26.9 Linux image/tools; never pull")
    args = parser.parse_args()
    try:
        if args.format_tool:
            format_tool(args.go)
            print(json.dumps({"status":"formatted-with-native-go1.26.9"}))
            return 0
        report = run_checks(args.repository, ROOT, args.go, args.checks.split(","), args.out, args.allow_dependency_download, args.docker_scanners)
    except (Refusal, OSError) as err:
        print(json.dumps({"status": "refused", "reason": str(err)}))
        return 1
    print(json.dumps({"status": report["status"], "checks": report["checks"], "performance_acceptance": False}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
