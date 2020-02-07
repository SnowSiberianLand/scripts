# -*- coding: cp1251 -*-
import mod_dmsrv as dmsrv
import entity_utils as eu
import mod_cmn as cmn
from collections import namedtuple
import mod_dm as dm
import mod_orm as db
import data_utils as du
import csv


interval = namedtuple('interval', 'bhname top base py_date')


def get_function_desc():
    return _("""
    Generate gis control perf 

    Generate gis control perf for tempest
    """)


def get_function_opts():
    return """
    CATEGORY=all;
    CAT_WEIGHT=1
    QUEUE=borehole; model;
    EXT=perf
    """


def export_data(ctx, res):
    #typedef ctx dmsrv.python_ctx
    #typedef res dmsrv.python_results
    result_ = True
    boreholes = ctx.ents.boreholes()
    model = ctx.ents.models()[0].getID()
    fileOp = open(ctx.path, 'w', newline='', encoding='cp1251')
    writer=csv.writer(fileOp, delimiter='\t', quoting=csv.QUOTE_NONE)
    writer.writerow(('Well', 'Date' , 'EVENT', 'MDL', 'MDU', '0.15', '1', 'SKIN'))

    mh = ctx.pStorage.getMetaHelper()
    dreg = mh.getDicRegistry()
    result_all = []
    #typedef ldata dm.ILogData
    for bh in boreholes:
        #typedef control_gis_table dm.ILogData
        #typedef res dmsrv.python_results
        #typedef res.err cmn.err_info
        #typedef cdata dm.ICompletionData
        tctx = cmn.progress_ctx()
        dthlp = dm.getDataProcessing().getDataTreatHelper()    
        cdata = dthlp.makeCompletionData(dm.db_caching, dm.cat_completion_events)
        b = cdata.load(ctx.pStorage, bh.getID(), tctx, res.err)
        citems = cdata.completions()
        nitem = citems.size()
        vec_perf_count = dm.vec_perf_interval()
        if nitem == 0:
            res.add_warning(_("No completion data"))

        control_gis_table = eu.load_prodloginterp_by_bh_and_model(bh.getID(), model, ctx.pStorage, res.err)
        if control_gis_table is None:
            res.add_warning("{0} prodlog data was clear.".format(bh.getName()))
            continue
        else:
            #typedef runs dm.ILogRuns
            runs = control_gis_table.runs()
            rsize = runs.size()
            if rsize == 0:
                res.add_warning("{0} prodlog data was clear.".format(bh.getName()))
                continue

            for row in range(rsize):
                #typedef run dm.ILogRun
                result = []
                run = runs.at(row)
                ldate = cmn.get_undefined_date()
                run.getLogDate(ldate)
                if ldate is None:
                    res.add_warning("{0} PGI without data".format(bh.getName()))
                    continue
                pdate = du.from_date_t(ldate)
                frames = dm.vec_log_frame()
                run.frames(frames)
                frame = frames[0]
                #typedef frame dm.ILogFrame
                npoint = frame.getNpoint()
                top_vec = frame.topMdVec()
                base_vec = frame.baseMdVec()
                if nitem!=0:
                    cdata.getCurrentCompletion(ldate, vec_perf_count, res.err)

                for i in range(npoint):
                    number = 0
                    topP = top_vec[i]
                    baseP = base_vec[i]
                    if True: #not cmn.is_undefined(consData[i]) and consData[i] > 0:
                        if len(vec_perf_count) > 0:
                            #typedef ival dm.perf_interval
                            first_ival(topP, baseP, vec_perf_count, "{0}.{1}.{2}".format(pdate.day, pdate.month, pdate.year), bh.getName(), number, len(vec_perf_count), result)
                        else:
                            result.append(interval(bh.getName(), topP, baseP, "{0}.{1}.{2}".format(pdate.day, pdate.month, pdate.year)))
                join_lock(result, result_all, 0, 0)
    for res in result_all:
        writer.writerow((res.bhname, res.py_date , 'PERF', round(res.top, 2), round(res.base, 2), '0.15', '1'))
    fileOp.close()
    return result_


def first_ival(p_top, p_base, pf_ivals, pdate, bhname, number, count, result):
    count = len(pf_ivals)
    ival = pf_ivals[number]

    if p_top < ival.top and p_base < ival.top:
        result.append(interval(bhname, p_top, p_base, pdate))
        return True
    elif p_top < ival.top and p_base > ival.bot:
        if count > number + 1:
            if not ival.is_isolation():
                result.append(interval(bhname, p_top, ival.top, pdate))
                return first_ival(ival.bot, p_base, pf_ivals, pdate, bhname, number+1, count, result)
            else:
                return first_ival(p_top, p_base, pf_ivals, pdate, bhname, number+1, count, result)
        else:
            if len(result) == 0:
                result.append(interval(bhname, p_top, p_base, pdate))
                return result
            result.append(interval(bhname, ival.bot, p_base, pdate))
            return result
    elif p_top <= ival.top and p_base <= ival.bot:
        if ival.is_isolation():
            result.append(interval(bhname, p_top, p_base, pdate))
            return True
        else:
            result.append(interval(bhname, p_top, ival.top, pdate))
            return True


def join_lock(res_in, res_out, index, cout):
        if index+1 < len(res_in):
            if res_in[index].base == res_in[index+1].top:
                return join_lock(res_in, res_out, index+1, cout)
            else:
                res_out.append(interval(res_in[cout].bhname, res_in[cout].top, res_in[index].base, res_in[cout].py_date))
                return join_lock(res_in, res_out, index+1, index+1)
        elif len(res_in) == 0:
            return True
        else:
            res_out.append(interval(res_in[cout].bhname, res_in[cout].top, res_in[index].base, res_in[cout].py_date))
            return True


if __name__ == '__main__':
    import mod_dproc as dproc
    import db_utils
    db.init_loggers()
    dproc.init_lib_dproc()

    ctx = dmsrv.python_ctx()
    res = dmsrv.python_results()

    ctx.pStorage = db_utils.make_DataStorage(db.db_sqlite, "C:/Users/Public/Documents/ResView-7.0.43 (x64)/demo.rds")
    if ctx.pStorage is None:
        print("DataStorage creation fails")

    models = dm.vec_model_t()
    models.append(ctx.pStorage.getRegHelper().getModelRegistry().find(20))
    bh_reg = ctx.pStorage.getRegHelper().getBoreholeRegistry()
    bh_vec = dm.vec_borehole_t()

    #rv_reg = ctx.pStorage.getRegHelper().getReservoirRegistry()
    #rv_vec = dm.vec_reservoir_t()
    #rv_vec.append(rv_reg.find(153))

    #typedef ctx dmsrv.python_ctx
    ctx.path = 'D:/export_.csv'
    ctx.model_id = (models[0].getID())
    for i in range(4, 5):
        bh_vec.append(bh_reg.find(i))
        break

    ctx.ents.append(bh_vec)
    #ctx.ents.append(rv_vec)
    ctx.ents.append(models)
    ok = export_data(ctx, res)
    print(res.err.msg)
    print(res.result)
    for v in res.text_comments():
        print(v)
    print()

    print("Result: " + str(ok))
