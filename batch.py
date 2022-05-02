import sqlite3, json, threading, time, multiprocessing, os, io, csv, contextlib

import run
from run import Case, Interface, Profile, make_from_json

TMP='/tmp/batch'

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        #print(o)
        if hasattr(o, 'to_json'):
            d = o.to_json()
            d['__class__'] = o.__class__.__name__
            return d
        return super().default(o)

class JSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        kwargs.update(object_hook = self.object_hook)
        super().__init__(*args, **kwargs)

    def object_hook(self, d):
        if '__class__' in d:
            cls = getattr(run, d['__class__'])
            return make_from_json(cls, d)
        return d

class Row:
    def __init__(self, case, rowid, files):
        self.case, self.rowid, self.files = case, rowid, files

    def run_on_in(self, wd, intf):
        cleanup = []
        initcwd = os.getcwd()
        try:
            for fn, data in self.files.items():
                path = os.path.join(wd, fn)
                with open(path, 'w') as f:
                    f.write(data)
                cleanup.append(path)
            os.chdir(wd) # make these references relative
            return intf.run(self.case)
        finally:
            try:
                os.chdir(initcwd)
            except OSError:
                pass

            for path in cleanup:
                try:
                    os.unlink(path)
                except OSError:
                    pass

class Database:
    def __init__(self, db):
        self.lock = threading.Lock()
        self.db = sqlite3.connect(db, check_same_thread = False)
        self.db.isolation_level = 'EXCLUSIVE'
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS cases (
                case_data TEXT,
                files TEXT DEFAULT NULL,
                claimant INTEGER DEFAULT NULL,
                output TEXT DEFAULT NULL
            );

            CREATE INDEX IF NOT EXISTS cases_claimed ON cases (claimant)
                WHERE claimant IS NOT NULL;

            CREATE INDEX IF NOT EXISTS cases_done ON cases (output)
                WHERE output IS NOT NULL;
        ''')
        self.je = JSONEncoder()
        self.jd = JSONDecoder()

    def add_cases(self, *cases):
        rows = []
        for case in cases:
            cur = self.db.execute('INSERT INTO cases(case_data) VALUES (?)',
                                  (self.je.encode(case),))
            rows.append(Row(case, cur.lastrowid, None))
        return rows

    def set_files(self, row, files):
        self.db.execute('UPDATE cases SET files=? WHERE rowid=?',
                        (json.dumps(files), row.rowid))

    def claim_cases(self, claim_key, size=64):
        assert claim_key is not None
        with self.lock:
            #cur = self.db.execute('BEGIN EXCLUSIVE')
            cur = self.db.execute('SELECT rowid FROM cases WHERE claimant IS NULL and output IS NULL LIMIT ?', (size,))
            rows = [i[0] for i in cur.fetchall()]
            cur.executemany('UPDATE cases SET claimant=? WHERE rowid=?',
                            ((claim_key, row) for row in rows))
            self.db.commit()
        cur = self.db.cursor()
        res = [None] * size
        for idx, row in enumerate(rows):
            cur.execute('SELECT case_data, files FROM cases WHERE rowid=?', (row,))
            jo, files = cur.fetchone()
            case = self.jd.decode(jo)
            res[idx] = Row(case, row, json.loads(files))
        return res

    def set_output(self, row, output):
        assert output is not None
        with self.lock:
            self.db.execute('UPDATE cases SET output=?, claimant=NULL WHERE rowid=?', (output, row.rowid))
            self.db.commit()

    def total_cases(self):
        return self.db.execute('SELECT count(*) FROM cases').fetchone()[0]

    def progressing_cases(self):
        return self.db.execute('SELECT count(*) FROM cases WHERE claimant IS NOT NULL').fetchone()[0]

    def finished_cases(self):
        return self.db.execute('SELECT count(*) FROM cases WHERE output IS NOT NULL').fetchone()[0]

    def clean(self):
        cur = self.db.execute('UPDATE cases SET claimant=NULL WHERE output IS NULL')
        self.db.commit()
        return cur.rowcount

    def reset(self):
        cur = self.db.execute('UPDATE cases SET output=NULL')
        self.db.commit()
        return cur.rowcount

    def iter_results(self):
        cur = self.db.execute('SELECT case_data, output FROM cases WHERE output IS NOT NULL')
        for row in cur:
            case, data = self.jd.decode(row[0]), json.loads(row[1])
            yield case, data

class Executor(threading.Thread):
    def __init__(self, db, intf, size=64):
        self.db = db
        self.intf = intf
        self.size = size
        super().__init__()

    def run(self):
        key = self.ident
        wd = os.path.join(TMP, str(key))
        os.makedirs(wd, exist_ok=True)

        while True:
            rows = self.db.claim_cases(key, self.size)
            if not rows:
                break
            for row in rows:
                out = row.run_on_in(wd, self.intf)
                self.db.set_output(row, json.dumps(out))

class Observer(threading.Thread):
    def __init__(self, db, interval = 10):
        super().__init__(daemon = True)
        self.db = db
        self.interval = interval
        self.die = threading.Event()

    def run(self):
        while True:
            if self.die.is_set():
                break
            total = self.db.total_cases()
            progressing = self.db.progressing_cases()
            finished = self.db.finished_cases()
            print(f'\x1b[1;32mRunning {progressing}, done {finished}/{total} ({100.0*finished/total:.2f}%)\x1b[m')
            time.sleep(self.interval)

def run_batch(db, lrmix, jobs=None, java=None):
    os.makedirs(TMP, exist_ok=True)
    if jobs is None:
        jobs = multiprocessing.cpu_count()
    exec_threads = [None] * jobs
    for i in range(jobs):
        intf = Interface(lrmix, java) if java is not None else Interface(lrmix)
        exec_thread = Executor(db, intf)
        exec_threads[i] = exec_thread
    obs_thread = Observer(db)
    obs_thread.start()
    for thr in exec_threads:
        thr.start()
    for thr in exec_threads:
        thr.join()
    obs_thread.die.set()

def row_to_contents(hdr, row, prefix=''):
    sio = io.StringIO()
    out = csv.DictWriter(sio, ['SampleName', 'Marker'] + [f'Allele{i+1}' for i in range(8)])
    out.writeheader()
    #print(out.fieldnames)
    nm = prefix + row[0]
    loci = {}
    for locus, allele in zip(hdr[1:], row[1:]):
        loci.setdefault(locus, []).append(allele)
    for locus, alleles in loci.items():
        row = {'SampleName': nm, 'Marker': locus}
        for idx, allele in enumerate(alleles):
            fa = float(allele)
            if fa == int(fa):
                fa = int(fa)
            row[f'Allele{idx+1}'] = str(fa)
            out.writerow(row)
    return nm, sio.getvalue()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Prepare, run, and export data from LRmix over multiple inputs')
    parser.add_argument('dbfile', help='The database to operate on')
    subparsers = parser.add_subparsers()

    def cmd_prep_siblings(args):
        if not args.population:
            print('At least one population file is required.')
            parser.print_usage()
            exit(1)
        if not args.replicate:
            print('At least one replicate file is required.')
            parser.print_usage()
            exit(1)

        db = Database(args.dbfile)
        pops = list(map(os.path.abspath, args.population))
        reps = list(map(os.path.abspath, args.replicate))
        entries = 0

        with open(args.siblings) as sibf:
            header = None
            for row in csv.reader(sibf):
                if header is None:
                    header = row
                    continue
                sampname, contents = row_to_contents(header, row, args.prefix)
                for pop in pops:
                    case = Case()\
                        .with_population(pop)\
                        .add_profiles(Profile('sib', sample_name = sampname))\
                        .add_evidence(*reps)\
                        .typical_hypotheses(sampname, args.contributors)
                    for hyp in case.hyp:
                        hyp.theta, hyp.drop_in, hyp.default_drop_out = args.theta, args.drop_in, args.drop_out
                    case.rare = args.rare
                    case.props['case'] = args.case
                    dbrow = db.add_cases(case)[0]
                    db.set_files(dbrow, {'sib': contents})
                    entries += 1
            
        db.db.commit()
        print(f'Prepared {entries} runs.')

    parser_prep_siblings = subparsers.add_parser('prep_siblings')
    parser_prep_siblings.set_defaults(func=cmd_prep_siblings)
    parser_prep_siblings.add_argument('-C', '--case', required=True, help='Case number to associate with this preparatory set (for plotting; can be anything otherwise)')
    parser_prep_siblings.add_argument('-S', '--siblings', required=True, help='CSV of siblings prepared according to Sibulator format')
    parser_prep_siblings.add_argument('-c', '--contributors', required=True, type=int, help='Number of contributors to the sample')
    parser_prep_siblings.add_argument('-P', '--population', action='append', default=[], help='Population files (can be specified more than once)')
    parser_prep_siblings.add_argument('-r', '--replicate', action='append', default=[], help='Replicate (evidence) file (can be specified more than once)')
    parser_prep_siblings.add_argument('-D', '--drop-in', type=float, default=0.02, help='Default drop-in to use')
    parser_prep_siblings.add_argument('-d', '--drop-out', type=float, default=0.05, help='Default drop-out to use')
    parser_prep_siblings.add_argument('-T', '--theta', type=float, default=0.03, help='Theta correction to use')
    parser_prep_siblings.add_argument('-R', '--rare', type=float, default=0.0, help='Rare allele frequency to use')
    parser_prep_siblings.add_argument('--prefix', default='', help='Prefix this string to each sibling\'s sample identifier')

    def cmd_run(args):
        db = Database(args.dbfile)
        lrmix = os.path.abspath(args.lrmix)
        java = None
        if args.java is not None:
            java = args.java
            if os.path.exists(os.path.abspath(java)):
                java = os.path.abspath(java)
                # Otherwise, it might just be in $PATH--leave it.
        run_batch(db, lrmix, args.jobs, java)
        db.db.commit()
        print('Done.')

    parser_run = subparsers.add_parser('run')
    parser_run.set_defaults(func=cmd_run)
    parser_run.add_argument('-L', '--lrmix', required=True, help='Location of the instrumented LRmix JAR')
    parser_run.add_argument('-j', '--jobs', type=int, help='Number of concurrent jobs to run')
    parser_run.add_argument('-J', '--java', help='Alternative JVM executable')

    def cmd_clean(args):
        db = Database(args.dbfile)
        res = db.clean()
        print(f'Cleaned {res} entries')
        cmd_status(args, db)

    parser_clean = subparsers.add_parser('clean')
    parser_clean.set_defaults(func=cmd_clean)

    def cmd_reset(args):
        db = Database(args.dbfile)
        res = db.reset()
        print(f'Reset {res} entries')
        cmd_status(args, db)

    parser_reset = subparsers.add_parser('reset')
    parser_reset.set_defaults(func=cmd_reset)

    def cmd_status(args, db=None):
        if db is None:
            db = Database(args.dbfile)
        t, p, f = db.total_cases(), db.progressing_cases(), db.finished_cases()
        print(f'Presently running {p}, done {f}/{t} ({100.0*f/t:.2f}%)')

    parser_status = subparsers.add_parser('status')
    parser_status.set_defaults(func=cmd_status)

    def cmd_extract(args):
        db = Database(args.dbfile)
        pops = set()
        contrs = {}
        for case, data in db.iter_results():
            pop = os.path.basename(case.hyp.p.population).partition('_')[0]
            contr = next(iter(case.profiles.keys()))
            case = case.props['case']
            for row in data:
                if row['Locus'] == '_OVERALL_':
                    pops.add(pop)
                    contrs.setdefault((case, contr), {})[pop] = float(row['LR'])
        with (
                open(args.output, 'w')
                if args.output is not None else
                contextlib.nullcontext(sys.stdout)
        ) as fo:
            out = csv.DictWriter(fo, ['Case', 'Contributor'] + list(pops))
            out.writeheader()
            for key, lrs in contrs.items():
                case, contr = key
                out.writerow({'Case': case, 'Contributor': contr, **lrs})
            fo.flush()

    parser_extract = subparsers.add_parser('extract')
    parser_extract.set_defaults(func=cmd_extract)
    parser_extract.add_argument('-o', '--output', help='Write to this file (instead of stdout)')

    args = parser.parse_args()
    if not hasattr(args, 'func') or args.func is None:
        print('No valid command.')
        parser.print_usage()
        exit(1)
    args.func(args)
