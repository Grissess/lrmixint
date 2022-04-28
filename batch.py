import sqlite3, json, threading, time, multiprocessing, os

import run
from run import Case, Interface, make_from_json

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
            print(f'Running {progressing}, done {finished}/{total} ({100.0*finished/total:.2f}%)')
            time.sleep(self.interval)

def run_batch(db, lrmix, jobs=None):
    os.makedirs(TMP, exist_ok=True)
    if jobs is None:
        jobs = multiprocessing.cpu_count()
    exec_threads = [None] * jobs
    for i in range(jobs):
        exec_thread = Executor(db, Interface(lrmix))
        exec_threads[i] = exec_thread
    obs_thread = Observer(db)
    obs_thread.start()
    for thr in exec_threads:
        thr.start()
    for thr in exec_threads:
        thr.join()
    obs_thread.die.set()
