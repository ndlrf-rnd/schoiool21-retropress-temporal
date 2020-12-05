# coding=utf-8
import csv
import io
import os
import re
import sys

IGNORE_VALUES = ('-', 'место для своего варианта разбора даты',)

MODE_RANK = 'rank'
MODE_HARDNESS = 'hardness'

DEFAULT_MODE = MODE_RANK
DEFAULT_REFERENCE_FILE_PATH = os.path.join(os.path.dirname(__file__), '..', 'corpora',
                                           'temporal-corpora-ru-rev3-resolved.csv')
DEFAULT_SUBMISSIONS_DIR = os.path.join(os.path.dirname(__file__), '..', 'submissions')

mode = DEFAULT_MODE if len(sys.argv) < 2 else (MODE_HARDNESS if (sys.argv[1] == MODE_HARDNESS) else DEFAULT_MODE)
submissions_dir_path = DEFAULT_SUBMISSIONS_DIR if len(sys.argv) < 3 else sys.argv[2]
reference_file_path = DEFAULT_REFERENCE_FILE_PATH if len(sys.argv) < 4 else sys.argv[3]


def sanitize_team_name(s):
    return re.sub(r'^[0-9]{4}-', '', s)


def load_submission(csv_path):
    sub = dict()
    with io.open(csv_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=',', quotechar='"')
        for rec in reader:
            sub[rec.get('Id')] = rec.get('Expected').strip()
    return sub


def get_hits(ref, sub):
    hits_set = set()
    ic_dict = dict()
    for k, v in ref.items():
        if sub.get(k, '').lower() == v.lower():
            hits_set.add(k)
        elif v and (v.strip().lower() not in IGNORE_VALUES):
            ic_dict[k] = v
    return hits_set, ic_dict


def calc_score(sub, ref):
    return len(get_hits(ref, sub)[0]) / len(ref)


reference = load_submission(reference_file_path)
hardness = {}
invalid_cases = {}
for idx in reference:
    hardness[idx] = set()
    invalid_cases[idx] = set()
results = []

for team_dir_name in os.listdir(submissions_dir_path):
    team_submission_dir = os.path.join(submissions_dir_path, team_dir_name)
    team_name = sanitize_team_name(team_dir_name)
    team_scores = []
    if os.path.isdir(team_submission_dir):
        for submission_file_path in os.listdir(team_submission_dir):
            fp = os.path.join(team_submission_dir, submission_file_path)
            if os.path.isfile(fp) and (submission_file_path.find('null') == -1):
                submission = load_submission(fp)
                score = calc_score(reference, submission)
                if score > 0:
                    hits, ic = get_hits(reference, submission)
                    for h in hits:
                        hardness[h].add(team_name)
                    for k, v in ic.items():
                        invalid_cases[k].add(v)
                    team_scores.append((score, submission_file_path))
        if len(team_scores) > 0:
            results.append(
                [team_name] + list(tuple(sorted(team_scores, key=lambda v: -v[0]))[0])
            )

results = sorted(results, key=lambda tt: -tt[1])
all_teams = set([tss[0] for tss in results])

if mode == MODE_RANK:
    print('\t'.join(['Score', 'Team', 'Submission']))
    for (team_name, score, submission_file_path) in results:
        print('\t'.join([str(score), team_name, submission_file_path]))

elif mode == MODE_HARDNESS:
    print('\t'.join(['Id', 'HitRatio', 'MissRatio', 'Reference', 'InvalidCases', 'HitTeams', 'MissTeams']))
    for (id, hit_teams) in sorted(hardness.items(), key=lambda kv: len(kv[1])):
        miss_teams = list(all_teams - hit_teams)
        print('\t'.join([
            id,
            str(len(hit_teams) / len(results)),
            str(1 - len(hit_teams) / len(results)),
            reference[id],
            ', '.join(sorted(invalid_cases[id])),
            ', '.join(sorted(list(hit_teams))),
            ', '.join(sorted(miss_teams))
        ]))
else:
    print('Invalid mode: {}, please, call script.py [hardness|rank] [submissions_dir_path] [reference_csv_path]'.format(
        mode))
