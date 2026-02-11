import os
import sys


RAW_ADDRESS = "12 D'Agnillo Dr., East Greenwich, RI 02818"


def _bootstrap_libpostal_library_path() -> None:
    if not sys.platform.startswith("linux"):
        return
    if os.environ.get("_LIBPOSTAL_BOOTSTRAPPED") == "1":
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))
    libpostal_libdir = os.path.abspath(
        os.path.join(script_dir, "libpostal", "src", ".libs")
    )
    if not os.path.isdir(libpostal_libdir):
        libpostal_libdir = os.path.abspath(
            os.path.join(script_dir, "..", "libpostal", "src", ".libs")
        )
    if not os.path.isdir(libpostal_libdir):
        return

    current = os.environ.get("LD_LIBRARY_PATH", "")
    paths = [p for p in current.split(":") if p] if current else []
    if libpostal_libdir not in paths:
        os.environ["LD_LIBRARY_PATH"] = (
            f"{libpostal_libdir}:{current}" if current else libpostal_libdir
        )
        os.environ["_LIBPOSTAL_BOOTSTRAPPED"] = "1"
        reexec_argv = getattr(sys, "orig_argv", None)
        if reexec_argv:
            os.execvpe(reexec_argv[0], reexec_argv, os.environ)
        os.execvpe(sys.executable, [sys.executable, *sys.argv], os.environ)

    os.environ["_LIBPOSTAL_BOOTSTRAPPED"] = "1"


def main() -> None:
    print(f"Raw address: {RAW_ADDRESS!r}")

    _bootstrap_libpostal_library_path()

    print("\nlibpostal.parse_address:")
    try:
        from postal.parser import parse_address
        parsed = parse_address(RAW_ADDRESS)
        print(parsed)
    except Exception as exc:
        print(f"  Error: {exc}")

    print("\nlibpostal.expand_address:")
    try:
        from postal.expand import expand_address
        expanded = expand_address(RAW_ADDRESS)
        for i, item in enumerate(expanded, start=1):
            print(f"  {i}. {item}")
    except Exception as exc:
        print(f"  Error: {exc}")

    print("\nusaddress.parse:")
    try:
        import usaddress
        parsed = usaddress.parse(RAW_ADDRESS)
        print(parsed)
    except Exception as exc:
        print(f"  Error: {exc}")

    print("\nusaddress.tag:")
    try:
        import usaddress
        tagged, label = usaddress.tag(RAW_ADDRESS)
        print(tagged)
        print(f"Tag: {label}")
    except Exception as exc:
        print(f"  Error: {exc}")


if __name__ == "__main__":
    main()
