import csv
import re
import kuzu
import os
import glob

data_path = os.path.join(
    os.environ['HOME'], "src/ldbc_snb_datagen_spark/out/graphs/csv/raw/singular-projected-fk")
db_path = "./test"

db = kuzu.Database(db_path)
conn = kuzu.Connection(db)


def create_schema():
    with open('schema.cypher') as fp:
        lns = fp.readlines()
        for ln in lns:
            ln = ln.strip()
            if not ln:
                continue
            print("executing: {}".format(ln))
            ret = conn.execute(ln)
            assert ret.is_success() == True


def adjust_edge_ep_coloums(orig: str, start: str, end: str, startCol: int, endCol: int) -> str:
    data = []
    with open(orig) as fp:
        cr = csv.reader(fp, delimiter='|')
        data = [row for row in cr]
    for i in range(len(data)):
        data[i][0], data[i][1], data[i][startCol], data[i][endCol] = data[i][startCol], data[i][endCol], data[i][0], data[i][1]
    data[0][0] = ':START_ID({})'.format(start)
    data[0][1] = ':END_ID({})'.format(end)
    modified = '{}.modified.csv'.format(orig)
    with open(modified, "w") as fp:
        cw = csv.writer(fp, delimiter='|')
        cw.writerows(data)
        print("adjust csv written to: {}".format(modified))
    return modified


edgePattern = re.compile('([a-zA-z0-9]+)_[a-zA-Z0-9]+_([a-zA-Z0-9]+)')


def extract_eps(edgeName: str) -> (str, str):
    m = edgePattern.match(edgeName)
    assert m != None
    return m.group(1), m.group(2)


def load_data():
    # COPY City from "dataset/sf-0.1/City.csv" (HEADER=true, DELIM='|');
    node_data_dirs = [
        ("Place", os.path.join(data_path, "static/Place")),
        ("Organisation", os.path.join(data_path, "static/Organisation")),
        ("TagClass", os.path.join(data_path, "static/TagClass")),
        ("Tag", os.path.join(data_path, "static/Tag")),
        ("Person", os.path.join(data_path, "dynamic/Person")),
        ("Post", os.path.join(data_path, "dynamic/Post")),
        ("Comment", os.path.join(data_path, "dynamic/Comment")),
        ("Forum", os.path.join(data_path, "dynamic/Forum")),
    ]
    for (nname, dir) in node_data_dirs:
        for f in glob.glob(os.path.join(dir, "*.csv")):
            print("loading node {} from file {}".format(nname, f))
            ret = conn.execute(
                'COPY {} from "{}" (HEADER=true, DELIM="|");'.format(nname, f))
            assert ret.is_success() == True

    # COPY City_isPartOf_Country from "dataset/sf-0.1/City_isPartOf_Country.csv" (HEADER=true, DELIM='|');
    edge_data_dirs = [
        # edge name, data dir, adjust column, start id col, end id col
        ("Place_isPartOf_Place", os.path.join(
            data_path, "static/Place_isPartOf_Place"), False, None, None),
        ("Organisation_isLocatedIn_Place", os.path.join(
            data_path, "static/Organisation_isLocatedIn_Place"), False, None, None),
        ("TagClass_isSubclassOf_TagClass", os.path.join(
            data_path, "static/TagClass_isSubclassOf_TagClass"), False, None, None),
        ("Tag_hasType_TagClass", os.path.join(
            data_path, "static/Tag_hasType_TagClass"), False, None, None),
        ("Forum_hasModerator_Person", os.path.join(
            data_path, "dynamic/Forum_hasModerator_Person"), True, -2, -1),
        ("Forum_hasTag_Tag", os.path.join(
            data_path, "dynamic/Forum_hasTag_Tag"), True, -2, -1),
        ("Forum_hasMember_Person", os.path.join(
            data_path, "dynamic/Forum_hasMember_Person"), True, -2, -1),
        ("Post_hasTag_Tag", os.path.join(
            data_path, "dynamic/Post_hasTag_Tag"), True, -2, -1),
        ("Forum_containerOf_Post", os.path.join(
            data_path, "dynamic/Forum_containerOf_Post"), True, -2, -1),
        # Country -> Place
        ("Post_isLocatedIn_Place", os.path.join(
            data_path, "dynamic/Post_isLocatedIn_Country"), True, -2, -1),
        ("Post_hasCreator_Person", os.path.join(
            data_path, "dynamic/Post_hasCreator_Person"), True, -2, -1),
        ("Person_likes_Post", os.path.join(
            data_path, "dynamic/Person_likes_Post"), True, -2, -1),

        # TODO(kikimo): missing Language node?
        # ("Person_speaks_Language", os.path.join(
        #     data_path, "dynamic/Person_speaks_Language")),
        # TODO(kikimo): ditto
        # ("Person_email_EmailAddress", os.path.join(
        #     data_path, "dynamic/Person_email_EmailAddress")),

        ("Comment_isLocatedIn_Place", os.path.join(
            data_path, "dynamic/Comment_isLocatedIn_Country"), True, -2, -1),
        ("Comment_replyOf_Post", os.path.join(
            data_path, "dynamic/Comment_replyOf_Post"), True, -2, -1),
        ("Comment_hasCreator_Person", os.path.join(
            data_path, "dynamic/Comment_hasCreator_Person"), True, -2, -1),
        ("Comment_hasTag_Tag", os.path.join(
            data_path, "dynamic/Comment_hasTag_Tag"), True, -2, -1),
        ("Comment_replyOf_Comment", os.path.join(
            data_path, "dynamic/Comment_replyOf_Comment"), True, -2, -1),
        ("Person_likes_Comment", os.path.join(
            data_path, "dynamic/Person_likes_Comment"), True, -2, -1),
        ("Person_workAt_Organisation", os.path.join(
            data_path, "dynamic/Person_workAt_Company"), True, -3, -2),
        ("Person_studyAt_Organisation", os.path.join(
            data_path, "dynamic/Person_studyAt_University"), True, -3, -2),
        ("Person_isLocatedIn_Place", os.path.join(
            data_path, "dynamic/Person_isLocatedIn_City"), True, -2, -1),
        ("Person_hasInterest_Tag", os.path.join(
            data_path, "dynamic/Person_hasInterest_Tag"), True, -2, -1),
        ("Person_knows_Person", os.path.join(
            data_path, "dynamic/Person_knows_Person"), True, -2, -1),
    ]
    for (ename, dir, adjust, startCol, endCol) in edge_data_dirs:
        for f in glob.glob(os.path.join(dir, "*.csv")):
            if 'modified' in f:
                continue

            print("loading edge {} from file {}".format(ename, f))
            if adjust:
                # import pdb
                # pdb.set_trace()
                start, end = extract_eps(ename)
                f = adjust_edge_ep_coloums(f, start, end, startCol, endCol)
            stmt = 'COPY {} from "{}" (HEADER=true, DELIM="|");'.format(
                ename, f)
            print('executing: {}'.format(stmt))
            ret = conn.execute(stmt)
            assert ret.is_success() == True


def run():
    create_schema()
    load_data()


if __name__ == "__main__":
    run()
