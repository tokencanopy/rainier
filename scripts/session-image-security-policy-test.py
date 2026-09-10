#!/usr/bin/env python3
"""Validate the public, test-only browser sandbox policy fixture."""

import copy
import hashlib
import json
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
SECCOMP = ROOT / "testdata/session-security/codex-bwrap-seccomp-docker-27.5.1.json"
APPARMOR = ROOT / "testdata/session-security/rainier-codex-bwrap.apparmor"

UNSHARE_USER = 0x10000000
UNSHARE_CHROMIUM = 0x10020000
CLONE_CHROMIUM_USER = 0x10000011
CLONE_CHROMIUM_ZYGOTE = 0x70000011
CLONE_CHROMIUM_ZYGOTE_NO_NET = 0x30000011
CLONE_CHROMIUM_PID = 0x20000011
# Chromium's safe-empty-dir helper uses the x86_64 clone optimization before
# chrooting its short-lived child. Keep this exact non-namespace shape rather
# than widening the Docker clone rule.
CLONE_CHROMIUM_CHROOT = 0x00084311
CLONE_WITH_NETWORK = 0x78020011
CLONE_WITHOUT_NETWORK = 0x38020011
ENOSYS = 38

DOCKER_CANONICAL_SHA256 = (
    "885442dc08f21f8d60f99ea43d59af88b1c529103815fe24bbf9ce998d3a609d"
)
SECCOMP_CANONICAL_SHA256 = (
    "4e43265c398ab8e93118568ff37749abd8d367dc6a01419dfa1d1efecd3bb636"
)
APPARMOR_SHA256 = (
    "53f78e768ee56099b764661c58b569504e39ecc45b2b3fb0dffd13ea329eb431"
)


def rule(names, action, *, args=None, includes=None, errno=None, comment=None):
    result = {"names": names, "action": action}
    if args is not None:
        result["args"] = args
    if includes is not None:
        result["includes"] = includes
    if errno is not None:
        result["errnoRet"] = errno
    if comment is not None:
        result["comment"] = comment
    return result


def exact_clone(value):
    return [{"index": 0, "value": value, "op": "SCMP_CMP_EQ"}]


class SessionImageSecurityPolicyTest(unittest.TestCase):
    def test_seccomp_is_default_deny_and_only_allows_pinned_namespace_shapes(self):
        profile = json.loads(SECCOMP.read_text())
        self.assertEqual(profile["defaultAction"], "SCMP_ACT_ERRNO")

        rainier = [
            item
            for item in profile["syscalls"]
            if item.get("comment", "").startswith("RAINIER:")
        ]
        amd64 = {"arches": ["amd64", "x32"]}
        self.assertEqual(
            rainier,
            [
                rule(
                    ["unshare"],
                    "SCMP_ACT_ALLOW",
                    args=exact_clone(UNSHARE_USER),
                    includes=amd64,
                    comment=(
                        "RAINIER: Codex 0.153.4 Bubblewrap creates its initial "
                        "user namespace with unshare(CLONE_NEWUSER)."
                    ),
                ),
                rule(
                    ["clone"],
                    "SCMP_ACT_ALLOW",
                    args=exact_clone(CLONE_CHROMIUM_ZYGOTE),
                    includes=amd64,
                    comment=(
                        "RAINIER: Chromium namespace sandbox launches its zygote "
                        "with CLONE_NEWUSER|CLONE_NEWPID|CLONE_NEWNET|SIGCHLD."
                    ),
                ),
                rule(
                    ["clone3"],
                    "SCMP_ACT_ERRNO",
                    errno=ENOSYS,
                    comment=(
                        "RAINIER: Chromium requires clone3 to return ENOSYS so "
                        "libc falls back to flag-inspectable clone(2)."
                    ),
                ),
                rule(
                    ["clone"],
                    "SCMP_ACT_ALLOW",
                    args=exact_clone(CLONE_CHROMIUM_ZYGOTE_NO_NET),
                    includes=amd64,
                    comment=(
                        "RAINIER: Chromium namespace sandbox fallback zygote shape "
                        "with CLONE_NEWUSER|CLONE_NEWPID|SIGCHLD."
                    ),
                ),
                rule(
                    ["clone"],
                    "SCMP_ACT_ALLOW",
                    args=exact_clone(CLONE_CHROMIUM_PID),
                    includes=amd64,
                    comment=(
                        "RAINIER: Chromium gives each renderer its own PID namespace "
                        "with CLONE_NEWPID|SIGCHLD after the zygote enters its "
                        "private user namespace."
                    ),
                ),
                rule(
                    ["clone"],
                    "SCMP_ACT_ALLOW",
                    args=exact_clone(CLONE_CHROMIUM_USER),
                    includes=amd64,
                    comment=(
                        "RAINIER: Chromium sandbox probes unprivileged user "
                        "namespaces with clone(CLONE_NEWUSER|SIGCHLD)."
                    ),
                ),
                rule(
                    ["clone"],
                    "SCMP_ACT_ALLOW",
                    args=exact_clone(CLONE_CHROMIUM_CHROOT),
                    includes=amd64,
                    comment=(
                        "RAINIER: Chromium's safe-empty-dir helper uses the "
                        "x86_64 CLONE_FS|CLONE_VM|CLONE_VFORK|CLONE_SETTLS|SIGCHLD "
                        "shape before chroot."
                    ),
                ),
                rule(
                    ["unshare"],
                    "SCMP_ACT_ALLOW",
                    args=exact_clone(UNSHARE_CHROMIUM),
                    includes=amd64,
                    comment=(
                        "RAINIER: Chromium sandbox creates its user and mount "
                        "namespaces with unshare(CLONE_NEWUSER|CLONE_NEWNS)."
                    ),
                ),
                rule(
                    ["clone"],
                    "SCMP_ACT_ALLOW",
                    args=exact_clone(CLONE_WITH_NETWORK),
                    includes=amd64,
                    comment=(
                        "RAINIER: Codex 0.153.4 Bubblewrap clone shape with a "
                        "private network namespace."
                    ),
                ),
                rule(
                    ["clone"],
                    "SCMP_ACT_ALLOW",
                    args=exact_clone(CLONE_WITHOUT_NETWORK),
                    includes=amd64,
                    comment=(
                        "RAINIER: Codex 0.153.4 Bubblewrap clone shape without "
                        "a private network namespace."
                    ),
                ),
                rule(
                    ["mount", "pivot_root", "umount2"],
                    "SCMP_ACT_ALLOW",
                    comment=(
                        "RAINIER: Bubblewrap constructs and discards a filesystem "
                        "only inside the child mount namespace."
                    ),
                ),
                rule(
                    ["chroot"],
                    "SCMP_ACT_ALLOW",
                    comment=(
                        "RAINIER: Chromium's safe-empty-dir helper chroots only "
                        "after entering its private user namespace; the kernel "
                        "still requires CAP_SYS_CHROOT there."
                    ),
                ),
                rule(
                    ["setns"],
                    "SCMP_ACT_ALLOW",
                    comment=(
                        "RAINIER: Chromium's namespace sandbox may join its "
                        "private user, PID, network, and mount namespaces after "
                        "creation; namespace ownership still limits targets."
                    ),
                ),
            ],
        )

        unconditional = {
            name
            for item in profile["syscalls"]
            if item["action"] == "SCMP_ACT_ALLOW"
            and not item.get("args")
            and not item.get("includes")
            and not item.get("excludes")
            for name in item["names"]
        }
        self.assertTrue(
            {"mount", "pivot_root", "umount2", "chroot", "setns"} <= unconditional
        )
        self.assertTrue(
            {"clone3", "sethostname", "setdomainname", "unshare"}.isdisjoint(
                unconditional
            )
        )

        docker_default = copy.deepcopy(profile)
        x86_64 = next(
            arch
            for arch in docker_default["archMap"]
            if arch["architecture"] == "SCMP_ARCH_X86_64"
        )
        x86_64["subArchitectures"].insert(0, "SCMP_ARCH_X86")
        docker_default["syscalls"] = [
            item
            for item in profile["syscalls"]
            if not item.get("comment", "").startswith("RAINIER:")
        ]
        canonical = json.dumps(
            docker_default, sort_keys=True, separators=(",", ":")
        ).encode()
        self.assertEqual(hashlib.sha256(canonical).hexdigest(), DOCKER_CANONICAL_SHA256)
        full = json.dumps(profile, sort_keys=True, separators=(",", ":")).encode()
        self.assertEqual(hashlib.sha256(full).hexdigest(), SECCOMP_CANONICAL_SHA256)

    def test_apparmor_keeps_docker_denials_and_limits_namespace_writes(self):
        profile = APPARMOR.read_text()
        self.assertEqual(hashlib.sha256(profile.encode()).hexdigest(), APPARMOR_SHA256)
        self.assertIn("profile rainier-codex-bwrap", profile)
        self.assertIn("  userns,", profile)
        self.assertIn("  mount,", profile)
        self.assertIn("  pivot_root,", profile)
        self.assertNotIn("deny mount", profile)
        self.assertIn("@{PROC}/self/{uid_map,gid_map,setgroups} rw,", profile)
        self.assertIn("@{PROC}/[0-9]*/{uid_map,gid_map,setgroups} rw,", profile)
        self.assertNotIn("setgroup?*", profile)
        self.assertIn("setgroup[^s]*", profile)
        self.assertIn("deny @{PROC}/self/", profile)
        self.assertIn("deny @{PROC}/sysrq-trigger rwklx,", profile)
        self.assertIn("deny @{PROC}/kcore rwklx,", profile)


if __name__ == "__main__":
    unittest.main()
