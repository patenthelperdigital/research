import argparse
from collections import namedtuple
import json
import pathlib
import re
from typing import Dict, List, Optional

import clickhouse_connect
import nltk
import pandas as pd
import tqdm


with open("stopwords.json") as f:
    stopwords = json.load(f)

Owner = namedtuple("Owner", ["name", "country_code", "individual"])
Patent = namedtuple("Patent", ["number", "owners", "address"])
Person = namedtuple("Person", ["name", "tax_number"])

client = clickhouse_connect.get_client(
    host="localhost",
    username="phd",
    password="phd",
)
client.command("SELECT version()")


def parse(row: pd.Series) -> Patent:
    row = row.fillna("")
    owner_names = [
        name.strip().replace("\n", "").replace("\r", "")
        for name in row["patent holders"].split("\r\n")
    ]
    address = row["correspondence address"]
    number = row["registration number"]

    regex = re.compile("\((?a:\w{2})\)")
    owners = []
    for name in owner_names:
        country_code = None
        individual = False

        country_code_match = regex.search(name)
        if country_code_match is not None:
            country_code = country_code_match.group(0)[1:-1]
            name = name.replace(country_code_match.group(0), "").strip()

        if row["authors"] == row["patent holders"]:
            individual = True

        name_parts = list(filter(lambda x: len(x) > 0, map(str.strip, name.split(" "))))
        if (
            len(name_parts) == 3
            and all(part[0].isupper() for part in name_parts)
        ):
            individual = True
        if (
            len(name_parts) == 2
            and name_parts[0][0].isupper()
            and name_parts[1].replace(".", "").isupper()
        ):
            individual = True

        owners.append(Owner(name, country_code, individual))

    if (
        len(owners) > 1
        and all(owner.individual is False for owner in owners)
        and sum([owner.country_code is not None for owner in owners]) == 1
    ):
        country_code = [owner.country_code is not None for owner in owners][0]
        owners = [
            Owner(
                " ".join(owner.name for owner in owners),
                country_code,
                False
            )
        ]

    return Patent(number=number, owners=owners, address=address)


def preprocess_name_for_exact_match(name: str) -> str:
    name = name.upper()
    name = name.replace("ИНДИВИДУАЛЬНЫЙ ПРЕДПРИНИМАТЕЛЬ", "").strip()

    match = re.search("([А-Я]\.)([А-Я]\.)", name)
    if match is not None:
        name = name.replace(match[0], f"{match[1]} {match[2]}")

    return name


def search_by_exact_name_match(name: Optional[str], address: Optional[str], individual: bool) -> List[Person]:
    if name is None or name == "":
        return []

    if address is None or address == "":
        address = "фыва" # meaningless, score ~ 1 (least similar) for ngramDistance

    orig_name = name
    name = preprocess_name_for_exact_match(name)

    stmt = """
        SELECT
            name,
            tax_number,
            ngramDistance(legal_address, {address:String}) as dal,
            ngramDistance(fact_address, {address:String}) as daf,
            dal * daf as score
        FROM search.search_base
        WHERE
            name = {name:String}
            AND individual = {individual:bool}
        ORDER BY score
        LIMIT 2
    """

    params = {
        "name": name,
        "address": address,
        "individual": individual,
    }

    res = client.query(stmt, parameters=params)

    if len(res.result_rows) == 0:
        return []
    else:
        return [
            Person(name=row[0], tax_number=row[1])
            for row in res.result_rows
        ]


def unquote_name(name: str) -> str:
    name = name.replace("«", '"')
    name = name.replace("»", '"')

    match = re.search("([А-Я]\.)([А-Я]\.)", name)
    if match is not None:
        name = name.replace(match[0], f"{match[1]} {match[2]}")

    if '"' not in name:
        return name

    parts = name.split('"')
    if len(parts) <= 4:
        return parts[1]
    else:
        return parts[2]


def search_by_like_name_match(name: Optional[str], address: Optional[str], individual: bool) -> List[Person]:
    if name is None or name == "":
        return []

    if address is None or address == "":
        address = "фыва" # meaningless, score ~ 1 (least similar) for ngramDistance

    stmt = """
        SELECT
            name,
            tax_number,
            ngramDistance(name, {original_name:String}) as dn,
            ngramDistance(legal_address, {address:String}) as dal,
            ngramDistance(fact_address, {address:String}) as daf,
            dn * dal * daf as score
        FROM search.search_base
        WHERE
            name LIKE {unquoted_name:String}
            AND individual = {individual:bool}
        ORDER BY score
        LIMIT 1
    """

    params = {
        "original_name": name.upper(),
        "unquoted_name": f"%{unquote_name(name).upper()}%",
        "address": address,
        "individual": individual,
    }

    res = client.query(stmt, parameters=params)

    if len(res.result_rows) == 0:
        return []
    else:
        return [
            Person(name=row[0], tax_number=row[1])
            for row in res.result_rows
        ]


def search_by_tokens_match(name: Optional[str], address: Optional[str], individual: bool) -> Optional[Person]:
    if name is None or name == "":
        return []

    if address is None or address == "":
        address = "фыва" # meaningless, score ~ 1 (least similar) for ngramDistance

    tokens = [
        token.upper()
        for token in nltk.word_tokenize(name)
        if token not in stopwords and len(token) > 3
    ]

    if len(tokens) < 3:
        return [] # method is slow, so we use it for long names only

    stmt = """
        SELECT
            name,
            tax_number,
            ngramDistance(name, {original_name:String}) as dn,
            ngramDistance(legal_address, {address:String}) as dal,
            ngramDistance(fact_address, {address:String}) as daf,
            length(multiMatchAllIndices(name, {tokens:Array(String)})) / length({tokens:Array(String)}) as ts,
            dn * dal * daf / ts as score
        FROM search.search_base
        WHERE
            ts > 0.5
            AND individual = {individual:bool}
        ORDER BY score
        LIMIT 1
    """

    params = {
        "original_name": name.upper(),
        "tokens": tokens,
        "address": address,
        "individual": individual,
    }

    res = client.query(stmt, parameters=params)

    if len(res.result_rows) == 0:
        return []
    else:
        return [
            Person(name=row[0], tax_number=row[1])
            for row in res.result_rows
        ]


def search(patent: Patent) -> List[Person]:
    result = []

    for owner in patent.owners:
        not_found = Person(owner.name, None)
        if owner.country_code and owner.country_code != "RU":
            result.append(not_found)
            continue

        if owner.individual is True:
            methods = (
                search_by_exact_name_match,
            )
        else:
            methods = (
                search_by_exact_name_match,
                search_by_like_name_match,
                search_by_tokens_match,
            )

        for method in methods:
            found = method(owner.name, patent.address, owner.individual)

            if len(found) == 0:
                continue
            else:
                result.append(found[0])
                break
        else:
            result.append(not_found)

    return result


def process_file(in_file: str, out_file: str, kind: int, chunksize: int = 1e2):
    input_df = pd.read_csv(in_file, chunksize=chunksize)

    for chunk in tqdm.tqdm(input_df):
        chunk_result = []
        for _, row in chunk.iterrows():
            try:
                patent = parse(row)
                persons = search(patent)
            except Exception as e:
                print(e)
                continue

            rows = [
                (patent.number, person.name, person.tax_number)
                for person in persons
            ]
            chunk_result.extend(rows)

        result_df = pd.DataFrame(
            chunk_result,
            columns=["patent_number", "person_name", "person_tax_number"]
        )
        result_df["patent_kind"] = kind

        if not pathlib.Path(out_file).exists():
            result_df.to_csv(out_file, mode="w", index=False)
        else:
            result_df.to_csv(out_file, mode="a", index=False, header=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="CH patent holders searcher",
        description="Searches patent holders using lookup table"
    )
    parser.add_argument("in_file", type=str, help="Input file (CSV)")
    parser.add_argument("out_file", type=str, help="Output file (CSV)")
    parser.add_argument(
        "-k",
        "--kind",
        type=int,
        required=True,
        help="Kind of patent (1 is invention, 2 is utility model, 3 is industrial design)",
    )
    parser.add_argument(
        "-c",
        "--chunksize",
         type=int,
         help="Chunksize for input file processing",
         default=10
    )
    args = parser.parse_args()

    process_file(args.in_file, args.out_file, args.kind, args.chunksize)

