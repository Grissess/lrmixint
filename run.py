import csv, subprocess, os

DEVFD = '/dev/fd'

# XXX eventually get rid of these constructor-bypassing shenanigans
class Dummy: pass

def make_from_json(cls, jo):
    inst = Dummy()
    inst.__class__ = cls
    inst.from_json(jo)
    return inst

class ProfileBindings:
    def __init__(self):
        self.map = {}  # profile -> drop out (possibly None)

    def add(self, prof, drop_out = None):
        self.map[prof] = drop_out

    def to_json(self):
        return {
            prof.sample_name: do for prof, do in self.map.items()
        }

    def from_json(self, jo):
        self.map = jo  # Will be fixed-up later in deserialization

    def fix_samples(self, mapping):
        self.map = {
            mapping[k]: v for k, v in self.map.items()
        }

class Hypothesis:
    def __init__(self):
        # Values initialized to LRMix defaults
        self.theta = 0.01
        self.drop_in = 0.05
        self.default_drop_out = 0.1
        self.population = None
        self.unknown_num = 0
        self.unknown_drop_out = None
        self.contributors = ProfileBindings()
        self.non_contributors = ProfileBindings()

    for attr in ('theta', 'drop_in', 'default_drop_out', 'population', 'unknown_num'):
        def setter(self, val, attr=attr):
            setattr(self, attr, val)
            return self
        locals()[f'set_{attr}'] = setter
        del setter

    def map_drop_out(self, do):
        if do is None:
            return self.default_drop_out
        return do

    def get_unknown_drop_out(self):
        return self.map_drop_out(self.unknown_drop_out)

    def each_contributor_drop_out(self):
        for contr, do in self.contributors.map.items():
            yield (contr, self.map_drop_out(do))

    def each_non_contributor_drop_out(self):
        for ncont, do in self.non_contributors.map.items():
            yield (ncont, self.map_drop_out(do))

    def args(self, infix):
        yield from [f'-H{infix}t', str(self.theta)]
        yield from [f'-H{infix}i', str(self.drop_in)]
        yield from [f'-H{infix}P', self.population]
        yield from [f'-H{infix}u', str(self.unknown_num), str(self.get_unknown_drop_out())]
        for prof, do in self.each_contributor_drop_out():
            yield from [f'-H{infix}', prof.sample_name, str(do)]
        for prof, do in self.each_non_contributor_drop_out():
            yield from [f'-H{infix}nc', prof.sample_name, str(do)]

    def to_json(self):
        return {
            'theta': self.theta,
            'drop_in': self.drop_in,
            'default_drop_out': self.default_drop_out,
            'population': self.population,
            'unknowns': self.unknown_num,
            'unknown_drop_out': self.unknown_drop_out,
            'contributors': self.contributors.to_json(),
            'non_contributors': self.non_contributors.to_json(),
        }

    def from_json(self, jo):
        self.theta = jo['theta']
        self.drop_in = jo['drop_in']
        self.default_drop_out = jo['default_drop_out']
        self.population = jo['population']
        self.unknown_num = jo['unknowns']
        self.unknown_drop_out = jo['unknown_drop_out']
        self.contributors = make_from_json(ProfileBindings, jo['contributors'])
        self.non_contributors = make_from_json(ProfileBindings, jo['non_contributors'])

    def fix_samples(self, mapping):
        self.contributors.fix_samples(mapping)
        self.non_contributors.fix_samples(mapping)

class Hypotheses:
    def __init__(self):
        self.p = Hypothesis()
        self.d = Hypothesis()

    def __iter__(self):
        return iter((self.p, self.d))

    def args(self):
        yield from self.p.args('p')
        yield from self.d.args('d')

    def to_json(self):
        return {
            'p': self.p.to_json(),
            'd': self.d.to_json(),
        }

    def from_json(self, jo):
        self.p = make_from_json(Hypothesis, jo['p'])
        self.d = make_from_json(Hypothesis, jo['d'])

    def fix_samples(self, mapping):
        self.p.fix_samples(mapping)
        self.d.fix_samples(mapping)

class Profile:
    def __init__(self, filename, hz = True, sample_name = None):
        self.filename = filename
        self.hz = hz
        self.sample_name = sample_name
        self._set_sample_name()

    def _set_sample_name(self):
        if self.sample_name is None:
            with open(self.filename) as fd:
                rd = csv.DictReader(fd)
                self.sample_name = next(rd)["SampleName"]

    def args(self):
        yield from ['-p' if self.hz else '--profile-no-hz', self.filename]

    def to_json(self):
        return {
            'path': self.filename,
            'hz': self.hz,
            'sample_name': self.sample_name,
        }

    def from_json(self, jo):
        self.filename = jo['path']
        self.hz = jo['hz']
        self.sample_name = jo.get('sample_name')
        self._set_sample_name()

class Case:
    def __init__(self):
        self.replicates = set()  # of evidence paths
        self.profiles = {} # sample name -> Profile
        self.hyp = Hypotheses()
        self.rare = 0.001

    def add_profiles(self, *profs):
        for prof in profs:
            self.profiles[prof.sample_name] = prof
        return self

    def add_evidence(self, *reps):
        self.replicates.update(reps)
        return self

    def with_population(self, pop):
        self.hyp.d.population = pop
        self.hyp.p.population = pop
        return self

    def typical_hypotheses(self, defendant, contributors = None, ddo = None, pdo = None):
        if contributors is None:
            contributors = len(self.profiles)
        prof = self.profiles[defendant]
        self.hyp.d.unknown_num = contributors
        self.hyp.d.non_contributors.add(prof, ddo)
        self.hyp.p.unknown_num = contributors - 1
        self.hyp.p.contributors.add(prof, pdo)
        return self

    def args(self):
        yield from ['-R', str(self.rare)]
        for rep in self.replicates:
            yield from ['-r', rep]
        for prof in self.profiles.values():
            yield from prof.args()
        yield from self.hyp.args()

    def to_json(self):
        return {
            'rare': self.rare,
            'replicates': list(self.replicates),
            'profiles': [prof.to_json() for prof in self.profiles.values()],
            'hyp': self.hyp.to_json(),
        }

    def from_json(self, jo):
        self.rare = jo['rare']
        self.replicates = set(jo['replicates'])
        self.profiles = {}
        for ob in jo['profiles']:
            prof = make_from_json(Profile, ob)
            self.add_profiles(prof)
        self.hyp = make_from_json(Hypotheses, jo['hyp'])
        self.hyp.fix_samples(self.profiles)

class Interface:
    def __init__(self, lrmix, java='java'):
        self.lrmix = lrmix
        self.java = java

    def run(self, case):
        rdfd, wrfd = os.pipe()
        rd = os.fdopen(rdfd, 'r')
        args = [self.java, '-jar', self.lrmix, '-o', os.path.join(DEVFD, str(wrfd))]
        args.extend(case.args())
        with subprocess.Popen(args, pass_fds=(0, 1, 2, wrfd)) as proc:
            os.close(wrfd)
        try:
            return list(csv.DictReader(rd))
        finally:
            rd.close()

if __name__ == '__main__':
    case = Case()\
        .with_population('/home/grissess/School/FST/fst_populations/Asian_FST_Frequencies.csv')\
        .add_profiles(
            Profile('C1.csv'),
            Profile('C2.csv'),
            Profile('C3.csv'),
        )\
        .add_evidence('REP1.csv', 'REP2.csv')\
        .typical_hypotheses('C1')

    # "as FST"
    for hyp in case.hyp:
        hyp.theta = 0.03
        hyp.drop_in = 0.02
        hyp.default_drop_out = 0.05
    case.rare = 0.0

    #print(' '.join(case.args()))
    #jo = case.to_json()
    #print(jo)
    #c2 = make_from_json(Case, jo)
    #print(' '.join(c2.args()))
    intf = Interface("/home/grissess/School/FST/LRmix/out/artifacts/LRmix_jar/LRmix.jar")
    print(repr(intf.run(case)))
