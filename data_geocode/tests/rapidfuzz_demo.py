from rapidfuzz import fuzz


def main() -> None:
    a = "greenlake drive"
    b = "green lake drive"
    score = fuzz.token_sort_ratio(a, b)
    print(f"token_sort_ratio('{a}', '{b}') = {score}")


if __name__ == "__main__":
    main()
