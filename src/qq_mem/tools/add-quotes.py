def main():
    lines = []
    with open("/mnt/ssd/query_workload/two_term_phrases/type_phrase.version_v2") as f:
        lines = f.readlines()

    with open("/mnt/ssd/query_workload/two_term_phrases/type_phrase.version_v2", "w") as f:
        for line in lines:
            f.write("\"" + line.strip() + "\"\n")


if __name__ == "__main__":
    main()

