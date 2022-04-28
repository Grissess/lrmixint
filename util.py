import io, csv, os
import run, batch

def row_to_contents(hdr, row):
    sio = io.StringIO()
    out = csv.DictWriter(sio, ['SampleName', 'Marker'] + [f'Allele{i+1}' for i in range(8)])
    out.writeheader()
    #print(out.fieldnames)
    nm = row[0]
    loci = {}
    for locus, allele in zip(hdr[1:], row[1:]):
            loci.setdefault(locus, []).append(allele)
    for locus, alleles in loci.items():
            row = {'SampleName': nm, 'Marker': locus}
            for idx, allele in enumerate(alleles):
                    row[f'Allele{idx+1}'] = str(int(float(allele)))
            out.writerow(row)
    return nm, sio.getvalue()

def make_cases(db, rows, pops, evids):
    for row in rows[1:]:
        for pop in pops:
            sampname, contents = row_to_contents(rows[0], row)
            case = run.Case()\
                .with_population(pop)\
                .add_profiles(run.Profile('sib', sample_name=sampname))\
                .add_evidence(*evids)\
                .typical_hypotheses(sampname, 3)
            # "as FST"
            for hyp in case.hyp:
                hyp.theta, hyp.drop_in, hyp.default_drop_out = 0.03, 0.02, 0.05
            case.rare = 0.0
            dbrow = db.add_cases(case)[0]
            db.set_files(dbrow, {'sib': contents})

def extract_overall_lrs(db):
    for case, data in db.iter_results():
        pop = os.path.basename(case.hyp.p.population).partition('_')[0]
        contr = next(iter(case.profiles.keys()))
        for row in data:
            if row['Locus'] == '_OVERALL_':
                yield contr, pop, float(row['LR']), float(row['LRLog10'])

def into_plot_csv(res, fn, cs):
    pops = set()
    contrs = {}
    for contr, pop, lr, lr10 in res:
        pops.add(pop)
        contrs.setdefault(contr, {})[pop] = lr
    f = open(fn, 'w')
    out = csv.DictWriter(f, ['Case', 'Contributor'] + list(pops))
    out.writeheader()
    for contr, rest in contrs.items():
        out.writerow({'Case': cs, 'Contributor': contr, **rest})
    f.flush()
    f.close()
