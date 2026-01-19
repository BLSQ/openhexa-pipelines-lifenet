import os
import shutil
import requests
import json
import polars as pl
import pandas as pd
from requests.auth import HTTPBasicAuth
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

from datetime import date, timedelta
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import period_from_string
from openhexa.sdk import workspace, pipeline, current_run, parameter
from typing import List, Dict, Optional
from urllib.parse import urlencode


@pipeline(name="Lifenet - HMISs")
@parameter("start_date", name="Start Date", type=str, default="2010-01-01")
@parameter("countries", name="Countries", type=str, multiple=True, choices=["BURUNDI", "DRC", "GHANA", "KENYA", "MALAWI", "UGANDA"], default=["BURUNDI", "DRC", "GHANA", "KENYA", "MALAWI", "UGANDA"])
def lifenet___hmiss(start_date : str, countries : str):
    #Lifenet Connection Identifier
    life_identifier = "lifenet"

    if "KENYA" in countries:
        #Kenya parameters
        k_conn_identifier = "kenya-hmis"
        k_org_unit_group_id = "maMqwNj9gNl"
        k_org_unit_ids = get_organisation_units(k_org_unit_group_id, life_identifier)
        k_data_element_ids = ['c2Gm5hW3I4i','OLlWZUfLtsR','t3QKFZbCf2B','iX5pEjcJowF','M9wNpvfC8wb','cV4qoKSYiBs','yQFyyQBhXQf','otgQMOXuyIn','DuBH6qPPdaO','BQmcVE8fex4','uHM6lzLXDBd','PggNwT09D3U','Nqt6rz4tqnm','gvZmXInRLuD','OoakJhWiyZp','aq64ukEqs2N','d3jNYZueSmn','JQkE0bN9OTB','SyUQl638r5P','QStt76mU06X','uTQOSp7rx5h','EsfvxFcltV0','Dx3XYket0sf','T22hPVk4yGZ','DpR59HXC0jW','GAeKNgKXNDm','WEW9Bjga52F','JGbmLfjZPEf','hAH08l33p9B','x13zGZKuxzs','F6AwnxbwiK2','rIkD2jIDqPh','Ha96SBKI06u','zXzBILTVJzD','pMbuvKvH4rg','p6QtFjN5M1R','qMYm7fGXkXP','rfmxXpWFwMq','fJ7CyT97iJC','MVIKFGNvNJp','QYUkPEhnhBX','tKGNp1722Hc','KH4eOYS7USz','MI3SdIsA0iU','r7LJ3kCX8EZ','cvJuw6Fbuiw','ijYBkFVUTMV','idiiaWIh2qi','ekh0oJidJ38','ovJmoErpCRY','dlldM4hP2Wk','KDFtI6rqYwI','BYMSIbnUzXQ','AC1Iorxdijc','dPRCstLVkZu','syjjPqXbjTm','ZYHcgOwtqAl','sEmbbCR882p','f9vesk5d4IY','etbuJ3chcZL','Fz0LzxMT1vV','qahRo2120r8','Lp9qb2iKLMJ','vA361bfq3S8','h14HsnrOBCE','p6QtFjN5M1R','ijYBkFVUTMV','qMYm7fGXkXP','rfmxXpWFwMq','hGhUdN3KgrK','ZI3qZ2mfBER','V6AhoeTTClY','B4jH9l9ZRKt','OvDwPPIrEnB','BmWgF3GJgIs','D923dTnerdk','lVNKL5UQ8Sb','FwsTJpdxT4C','qrTk1w6Y5Bk','idiiaWIh2qi','ekh0oJidJ38','f9vesk5d4IY','etbuJ3chcZL','Fz0LzxMT1vV','hxXwYIgT8rI','f9vesk5d4IY','y6Vi9uqabo4','xNW6Y9Kitok','k8qXy4LyyJj','ijYBkFVUTMV','Kx64gGqaFVq','sMqM8DwiAaj','jaPrPmor6WV','rAZBTMa7Jy3','UqKC1DJnymn','rYn8RCcHJg1','wZtDYVBa41h','GAr6xu6f1n7','KDFtI6rqYwI','BYMSIbnUzXQ','AC1Iorxdijc','dPRCstLVkZu','UiGlnkNmCut','UsyFvMBxvn0','KuMX8VqCejs','Gp38RIEAOCb','Gw8C0uUQade','pp5FTNgUXmf','wxg6cuH9cXF','VT8eVxvtphF','mdNncoz3lFe','fJ7CyT97iJC','MVIKFGNvNJp','B2Pfs8oAuiy','YD9L08SeDPE','kfuGsBdD3rp','qcKLKy25k4G','O3qECFGrzeF','QYUkPEhnhBX','tKGNp1722Hc','KH4eOYS7USz','MI3SdIsA0iU','gw4Zog5qvBy','ku80YRejJPg','K1Zzhafeukq','OyddF1qflzP','g1tOm44qfsi','Ba7pFRVTVnC','TLOb9t8hfAp','IEl87u0hwfZ','sGqznpDvcmt','zVjl7nB15Kx','sRbqc5gBQhI','Jw3alL29ZEi','VuXfcCV7RtR','tBPGQFhWB7M','dncP9CPM8QS','dlldM4hP2Wk','xAAEwRrU8EH','BkzYqWdGikV','dncP9CPM8QS']
        k_data_element_groups = ['ofBSokyPXNn','SMqDYR0nUcg','Qh1W8QRaFUZ','E40Npg5eHsE','n0fE9qoi5N1','qADpr4dQZuC','JMNedIzqWGA','Tco6NWSjw53','XUyBl4L2raq','VWCIP8wQvMc','dTUuUMw64bk','SWuN3c3i8fL','wqLiG4JdCcO','VbAGjhNkaOw','XIhYn40pCwk','DCnQwGyolzD','gT7SBAw0EMU','ZEly6FddL4n','ig1GVX7tNmh']
        # Get Kenya Data
        get_data(k_data_element_ids,k_data_element_groups,k_org_unit_ids,k_conn_identifier,start_date)
        current_run.log_info("== No Kenya")

    if "UGANDA" in countries:
        #Uganda parameters
        u_conn_identifier = "uganda-hmis"
        u_org_unit_group_id = "rP6kTiTZ3SD"
        u_org_unit_ids = get_organisation_units(u_org_unit_group_id, life_identifier)
        u_data_element_ids = ['Dem2BmAhrEl','YNqGVS6GEyo','aU0n2RXwQJC','UZik6fPnd6V','qum4x1wSYNE','wUDxFVBapIc','LiSyn6bblsc','Kz1EDWfsQvI','IbiK1GS74qv','ywfVj7F30Ts','QQxE1VhHaGo','Uy4JaNUAtmr','GqvGVF75ihM','LSrwJqzz8h4','HWsIOGrZuee','hTBgki6DhH4','G3qsihJWUFM','CUAumK84NhX','TFfHVzlaZTw','C74ipmVVinR','dZMr4ggXHaf','Z9v6RRvF29P','DuZYoeTALr9','dCNNhoMn3rv','ShYudJcDTwp','PNXE7N7CH7u','uUYRrEU5iOB','L4pwIgSDdG6','AGmXoLiT89x','KDoEmOjpYnL','wOvonjgfh4b','OUGMxrtXxri','DivdIXHxiik','OG2VmPZSmyv','vFHITfiDo9g','B404cBe6vUa','qum4x1wSYNE','vI52cxgsBNv','A2ZOozUNmDz','TFfHVzlaZTw','CUAumK84NhX','G3qsihJWUFM','hTBgki6DhH4','HWsIOGrZuee','LSrwJqzz8h4','GqvGVF75ihM','Uy4JaNUAtmr','Uo3DcCS7GQJ','QQxE1VhHaGo','ywfVj7F30Ts','Kz1EDWfsQvI','LiSyn6bblsc','wUDxFVBapIc','qum4x1wSYNE','UZik6fPnd6V','aU0n2RXwQJC','YNqGVS6GEyo','MzqiroyuhJh','bl6lQqygEK1','P1MyPWVxi5T','F8Iz6QcexWB','SFMipMUafSk','idXOxt69W0e','MxAg9De4cra','NYs9Ct3X4FD','eEllJg5kQn6','G4uZN2Y4oAG','Quc9uIBIxk6','HLyPaHaoHSu','ehj9x0CXNN5','Ys31ug5E3f1','z1s4aIzf8ga','ujs4ipzA4tb','NbG01i1Md55','dmSXPXTnV0e','QHawVF72X6E','rDZlacXXUPM','QFolEgl8SEB','WAjgHQVxVVm','yctOQDdXZav','sv6SeKroHPV','sQ4EexvvhVe','UwnR5kr982Y','PaceRdSpmgy','dt0srLW7MmK','A3r248imZ1L','dyfJZ2Llr6F','gsl7Ws1pTg1','','NXCJqj7Edfo','ZVKD2XRg6Ab','Fat3y7a43qY','iOdD0RgSRoD','O3dDxoUjSqo','fCj5K4Xqy7T','BsyuYs7db5P','pGndkat5h9s','jSQRBvez5Pq','zhTC6HT9No1','VHi82Mcd77w','ibfLXaygcNS','HnSiI2XX2Sy','qSe7OiDB18A','Onpo1O1A7jr','bkCFVYMFMed','jNpa9MJhPZm','lUH7AWlsExq','LJ8MLXHctGs','RYcEItpNCUp','T8W0wbzErSF','ULL9lX3DO7V','hrTskGHP0Av','WAdw7P2USJV','fEz9wGsA6YU','nnmOsAUssg9','h5qpxtCVwAp','pWZcOnKaoCa','ZUpS5SUz7Zf','NiFpmskP3EW','Q9nSogNmKPt','RnLOFSYaAhp','tsJBUFW2Vjm','hEvz6VY4COC','JOWj87d62MK','qCXtnqhwKak','GwQD5HqHGIv','IbiK1GS74qv','HkI0xDwvvr8','XwydjcQlZOI','KzBNR8ia9ot','xzZZOy6cmr8','XXZZbU4B2N3','Tf0xH5TZftt','tfQukLqcJz9']
        u_data_element_groups = ['cBy6DInBpkj','PQKBXQDx4ai','nOUV3WjbnqZ','uxgAM2mL2XG','W1NQYn9zeT0','Wisyk46hHeq','PmKAZf4sASl','BXvdL8o2RSh','a8zBKLRQJqX','qpIxOxGdXTh','Zo9TmJGnZRe','vPvLSIPrfnb','W1NQYn9zeT0','NMFTigF3Xkt','Q0c1uezRn4g']
        # Get Uganda Data
        get_data(u_data_element_ids,u_data_element_groups,u_org_unit_ids,u_conn_identifier,start_date)

    if "GHANA" in countries:
        #Ghana parameters
        g_conn_identifier = "ghana-hmis"
        g_org_unit_group_id = "rX8bkQ79b7i"
        g_org_unit_ids = get_organisation_units(g_org_unit_group_id, life_identifier)
        g_data_element_ids = ['tAs56Gg7OIN','ErXpaxK9G4E','JcN8CtbHFYc','QZZSemnvdlU','glWIq7TGk1T','SiJyJ4JCQd7','VF7AMOEqzLT','NPa2o1TmwXT','va7C32YtV1s','A6d9xWlgaVm','lQdkCVe52cA','j865ugazSzW','WT0t3CwwQ4z','Uqvd24Qbhr3','aqNSxKPnuKG','leXgOfhDXcW','J0RLc01hqsy','aqNSxKPnuKG','J0RLc01hqsy','ErXjO6hPMpT','IEhuO68SZvv','dOCm9jNGNjP','ErXjO6hPMpT','DV1bQepEy3x','r4KFJAowmEs','N0HmpmVlUYX','ni0Bb1d6PRT','PDYu4HOCKDR','Zb5mJLvEuPa','F7AMOEqzLT','xtaSuCPILyv','VXKOZ8l189S','r2RLwnmYuny','uuFVAVYRgVR','b42gjU67Sjs','oMA2DSTV60a','w2SXzkGFNAv','nAGqJMEryzP','iMkTm9fppFq','jdQO1eMLJ7o','jdQO1eMLJ7o','B4YKL9do5nW','KwsRZrJWLjy','N2oCVch5wi6','O7hxRRtqaF2','GxaJ5Rx6DDS','W6Ks1aeBS0J','zKazCXxXQd0','jZPuvtmMmnv','ENATIu8S3Jw','eeokS2wf8fV','xCsGElKMxYg','vCRofiYqjTm','uxzQ7PHiTWu','JBhPqpQVstu','LFOQk9pu2Rp','p8oTUw96Kn1','G5MPxkVkVJz','uMafJkdRFgV','IcvKY9iQ0lB','kNLSO4ertuc','Djmlot4lRCP','e6GvKVepS9i','aaslLDPSKvP','oYLI26sVDwX','JHNyPIYxFUr','zas3T6w0FUT','w3LTVgrV5XL','HIOeUlOq2fo','s7sGECxfKXr','YFcGtDsZnmP','fzUgWReEMqz','WnQDYB54CrM','XK0fpGSCUJP','Z2bsh5oMBEc','bOTVwBwbv23','jHG7YHGOLQN','DCphQxDTQIZ','Ue1pZlr1YDZ','jixY1K8JvAd','Aog3jaMTvo7','sOEJ2Fm2I1n','qv7esJxt7dU','fV03YjY8qfx','ucYeWgVmh0G','TBQT8Bivyvo','aqNSxKPnuKG','leXgOfhDXcW','J0RLc01hqsy','ErXjO6hPMpT','IEhuO68SZvv','dOCm9jNGNjP','a3gFm1M2i8S','ezvxdn1T7ie','BH9lB4FlL2K','ABzefRpeKh7','lFEllQfDdhF','nTyzF6AzCO9','TjAs4iyxIUh','LRkd9defLgz','Aecm12zUzZ0','vLwkFKTzHJv','dOgZGgucwyu','mPGvBR9KvYC','glWIq7TGk1T','SiJyJ4JCQd7','CFcb6iFTvms','hIDhWbrli42','TW9czUWSlP7','oEcZpxk7Rf3','iWoeSCakQaE','TnMryyPjk6T','rvAykfczHRi','KypKeCZLDgR','HRloFZqfG5B','iPXFJfsJDTd','hHDud7w6330','g2KgXPM3ipk','Z993NkZima8','MFioLhQXlXt','A6kORjQ3fbc','HsUHBWG0t4M','QrL0nT8aBIZ','GGJC8yHTgG0','LlDVZFo4fKF','Yj3CvImgye6','WSEAfuLyxMZ','r3gJ6naFz44','wXZTi4HU1UU','tAs56Gg7OIN','ErXpaxK9G4E','E4TOOJajLta','M30fe90zqP5','IPZ5slSg5WQ','xcXQtQ0SEhm','S5fCNtU9Rx0','BkPLgZ6332Q','jdxvRz2ER79','bRs0F1NfmDG','RbKUJAe04DP','l9ypN2TOXS1','SzYUXUum9hD','w5bWyShTRda','kExPsDMDVVA','Uokq3G5dVBp','XrL5bKOgej8','xrB0tfWxbsm','NycE4uys6hY','C1dNDN3VFAV','w6VDsTH0wW9','T40BcJkQHtj','PPgBRKOI44q','wwEkOuMluUq','UmyD6nF6NHG','KcuB4aMv9gJ','gvI1z5wjd4W','cIDN3Z84HHR','H4riF3Fdl4E','MdQj9sVKTrw','WPsmra5FomD']
        g_data_element_groups = ['bKnH5cjibju','qvJ4lfY39B8','AIR3PevnkX1','iNKQ94YFAi2','CrbRhxAjC7I','BKV7si1tSyb','Y5bBv4wOO54','zuwpJUFpWPq','bKnH5cjibju','l7x4Th8Yyjk','ao8xuAkvB43','e6vgHQeJ3T1','wFAWlGYTcX4','YGpjoeCmLnW','oI1dhPjSGiu','kWEUURm4mdL','bKnH5cjibju','BvSZct8R9nK','AfN57jhF423','RzLaF1jmWeZ']
        # Get Ghana Data
        get_data(g_data_element_ids,g_data_element_groups,g_org_unit_ids,g_conn_identifier,start_date)
        current_run.log_info("== No Ghana")

    if "MALAWI" in countries:
        #Malawi parameters
        m_conn_identifier = "malawi-hmis"
        m_org_unit_group_id = "XyfPVCdqOwo"
        m_org_unit_ids = get_organisation_units(m_org_unit_group_id, life_identifier)
        m_data_element_ids = ['XLn9qRDIDGg','gCP7WoODQqY','i7EICUDBS9M','D3m5WvOSixM','TBjgn1GCFOO','dTK0MlakKzU','B3b4hGwWxyH','ZiB49TYkaAE','iBBnHx1Uf50','EeywK6AHQdK','jz793Wzon1W','q7NoDcfaciO','kaYz6eqxqQH','bjLZY1FqdDJ','MD0Gp8MzkHf','xRrJWogiEMf','U3FydV1M1RQ','PpmPCtwOWNR','a3DzFDGAQAi','l2GWTPGK4QP','WjHvEHMCyKo','Hcho5dCr','ynY0QLNPFpn','ftyQGpirFHE','lM48Ysgzz0H','p7SpeubB59p','QD9JBJ6ZakN','o8fLTxlRPzv','JYYgpjhp0qN','Pni0DP4zlWJ','BIZneYMhHku','iBfokcDFZnI','ueZ9XGbE7Dn','us7AbR44NDw','YGHvR4UKMwB','F1DkHBIW7tO','BjOKMvEcpXM','VEgce5WnxHV','Kp3yd5v3rec','lPvOHlySRwG','x5RtjHfY0gI','wRnWFfvI3E8','yCJBWuhuOLc','kz8lbDDDz34','NZzRHJ6tmPE','MoximPvXuj6','l2GWTPGK4QP','bPvw8ZdBgRe','EJ0qYm1FrcH','YlydKJIyGFF','jz793Wzon1W','bjLZY1FqdDJ','QiIfrKC5Ox8','XgYKomvdkBo','xgW1P5I9cN2','NeEMZqYtG4c','NxggSy8gzqo','DQNn56buEd5','hfihixkbjhh','bycjbqrbtoz','qphfxmgmvug','e5ETCXCmBaf','QQoLFyarP03','OvDhfIIJPS7','jxaojehpgby','dqmDYv3P8Bi','PfNXkoOulIo','zwCJtAIraUB','JpGmb8ofLAv','JtnAK5uZU4Q','pfuJANCCQOc','ueC8Esnjd2M','Lwav7YQBgjC','vcEWpcFjEuU','BtjzKJIuTJv','Y2vzWp3rPce','NtkuSvtY8D0','TAsjIcQVYG6','eoS9HXn97nF','bYV6cz5L9Kz','KOu770F7YOQ','hPhTbuLFnx4','YIP4fvYcGYQ','dTK0MlakKzU','yOOBXy8nGXF','hqAy9jJ6gmT','CKuA0GiXFMS','xnuL680PHT4','jrdZDuwSrzc','mHSfE7GArgh','QvexDntI3mf','v2I6dWgLYrM','QVMyrWnXwYU','TSCmQaQwcDx','KiwtHd7nWCX','PkrtOx1olaV','mraYTWoViTj','SVC5nKhvcQr','ZgDrKbIPkgE','Jtrq0pYsqT0','Jpkh8pokAY7','EUvtRZSLh2F','Z73ODYforG3']
        m_data_element_groups = ['FXA7FbbKvZ1','A5xW5sIeoRV','l2cbutKQU9K','FnOzKaLnGy1','TJ7XX2vEtyd','SLDYafSzSrj','xiMFKdTWRFp','tsefi6doleb','AnGLAlOrzpN','vJxPpUXNNci','sl3HwATctcg','eeWCk7FjSKE','HRWNG10u4OR','FrXg1nHZxL2','kkHokihKMp2','t2MyS9B4IoV','mQ9GjwjE9wM','IGEQm1wP1dJ','RHgqynGGPy1']
        # Get Malawi Data
        get_data(m_data_element_ids,m_data_element_groups,m_org_unit_ids,m_conn_identifier,start_date)
        current_run.log_info("== No Malawi")

    if "DRC" in countries:
        #DRC parameters
        d_conn_identifier = "drc-snis"
        d_org_unit_group_id = "K74ZpyPIZEI"
        d_org_unit_ids = get_organisation_units(d_org_unit_group_id, life_identifier)
        d_data_element_ids = ['YxweVLhwnbl','YoDGC0EJNyq','teFRH9MsoGm','l3TXTsIVYEv','IPnqGkUUN6U','rfeqp2kdOGi','tTK7nV64EvX','DQiMxAlXTOe','SZhsGuHCYEL','wwde2QtlIRN','WWxqaHSeiwd','LspDmk116zD','zcH4sipFcUr','qFbtP6YBqkJ','smQqPyAxDiJ','wuZyPPSVrc3','UxD03qX5O0t','WgZr7FrDfVn','AJklQZlDGEp','jDH08sGWeGB','QcpvBIDizo9','DdGZRAAvldF','jK7FLKWMCnC','TTK7nV64EvX','DqiMxAlXTOe','AxJhIi7tUam','aK0QXqm8Zxn','IPnqGkUUN6U','rfeqp2kdOGi','hoPZUCWGbvD','BjzoMcHdu1k','e8kkhUinpNJ','kEUOeuRvDWk','zLeY0EViRHg','UxD03qX5O0t','NJw6RhEcKIS','gtsulcE7ena','kGmdIbd3mbn','NiNF1GtNTkd','j5VRZFJzUli','OLme8tEPGov','MuIfWlqJ0Cf','aoIpDzKttLJ','O0eYKOFhMee','gDnbubwzuts','bSINVpDbI6c','QRs9kVKS047','AjklQZlDGEp','j2DKCdQwurw','i8LtPxBnqnV','n2k6Sbtwtgh','I1x8R4StgSA','L0GHHIlZDIq','zcH4sipFcUr','qFbtP6YBqkJ','bOfjRt0MPZn','SzhsGuHCYEL','Jr1CDZh5BKh','a8YKnKG0JXZ','ELYjn1wkdnI','UgI0jQybW1l','zrsma7SQkbW','LAFIsrZWPof','uKxLt4S9xUP','W25tOXS0rxS','f1MiAAIu366','j3P9bSnwF12','ozVdkud4F03','SPGaDDw2JzG','I6tLH3nk9tw','bzD4QxaNkJm','eoIUv4bRBOk','jqAIZmVwkZd','smQqPyAxDiJ','ImmKmjpMW6u','MzeK7PxCDSM','sUVL3kym5uR','Q2hgxeRH7uX','O4zM5PA27NE','gihu8MW6ga6','xnB0V4LRCoA','WOm7ro9YniO','EhshL3PqSAh','muGZqMdLcJz','N60LF19AcTf','p3HvbM5WquI','WlTa2LjlF8q','Jcb2h160gb0','cPAApegfYAZ','PuTEbadhAPf','i5zmivDIHN8','wwde2QtlIRN','cTLKwfG8pSv','J8R3WFpMAZI','WGu60o5mePq','CgR8kWNdK3W','WwxqaHSeiwd','YYKXOc7xBUi','uNdFg1eymsa','YBnkF8oFyBO','M2JQW0H44dI','Y7zTdjUfSlz','hxTYMhz2fYf','e1hDWsq2olK','jEJ1LzB0BLZ','Z86bHthxwVG','v7DDmc4zsWu','nNYSsMt45hw','LspDmk116zD']
        d_data_element_groups = ['IMHGy8MX3wG','PRlhYClbAs7','alU5ulLcmLJ','aBM1ANXpbpC','RtDEQS6sbcq','xFUJ5K2JF4r','fzXSfcghR9h','rlmzmjiUR3W','pf2NTDFHym1','QSElAHIYbOU','qIMdBFn5Uvd','LgPxtZ1JSjH','pRGFD7tsGz2','LgPxtZ1JSjH','e2G7cfoNZ4G','qCFRiss9p9v','lxb21pmYdqw','xJCY73ee2vN','tDKaxgERUAR','WytfXnKt73I','wmzXaVyWASM','B2l6jh9FYgh','fZx86mbqEv3','VXjXo78YHz4','E3nAmNYoFzM','aPYWzIrR4Rh','zoYXWaxdkCi','GewECwrQK4t','LTuiwzK2XYO','o0C4iIwXeJc','xFF02gBEhPR','EUEZWxAmccl','d3R2MH1m0RI','JG9Vqun6FzO','fkVmBwwOIeo','PAk8ACdsgp3']
        # Get DRC Data
        get_data(d_data_element_ids,d_data_element_groups,d_org_unit_ids,d_conn_identifier,start_date)
        current_run.log_info("== No DRC")

    if "BURUNDI" in countries:
        #Burundi parameters
        b_conn_identifier = "burundi-hmis"
        b_org_unit_group_id = "e6KX5E5Oe9c"
        b_org_unit_ids = get_organisation_units(b_org_unit_group_id, life_identifier)
        b_data_element_ids = ['tPMq4rxV41G','WMqO9UJBJSw','lwPXUOFmtXQ','tEtMeklU5rR','CfZTmDwZUh7','zL3XlRoXfWa','I5mp2W8knSC','rA9evRpaSlD','erKHFLwGwMp','s6Ca1YtxhXf','wviY7qewvVh','yB0Cgd3dEsR','uf9Y9jXaDQw','OkCvVEWt2wj','OIfB5ZAtT34','V1tSt9NroeS','w03GjKm6up9','QOlFB3Ev4Jo','h9EytkY6M2z','h9EytkY6M2z','Kg32Hb5UpUC','Kg32Hb5UpUC','vp9olxQ1dv9','S8RjnQDBObs','GyT9slNFY8D','ghN2d6CroMJ','BXl88CcTZHO','GyT9slNFY8D','ghN2d6CroMJ','BXl88CcTZHO','cgGBTu6Xm65','kuTbYtSy3Pp','CJi5bewIsNN','SKit63CLTux','bJ75Lbz0Qss','J5fW63Jsd04','CZwF141N3gX','RYYCIBCP8Pe','JJmxhoIxByc','SFTpvzIq0P5','e1abJ5ezYzK','Y3RQAtpnJ3O.rqQuGU2IAs6','Y0alfPyZ81U','NYd6KIjGS3Z','emF0ll8UN2T','OES4dSgIBxW','ur0FrVtmhPG','SDm3VVG1jMp','sc8fs4GfcOy','OEy7p30izz1','DCldsat9Igd','M0bYQyYeYMQ','lMQRe6ez8rM','qm29orau9fC','JarzwzBpEgo','EDEIzAre9gR','niBkwVXCx3q','Z0l9LkFuuym','YXQhcmUdPFb','tmxhqBiX1hC','fqItdPDbDoo','fqItdPDbDoo','sHUojx0LgOV','xIDB6prYVYw','hqw08UsCoEn','irsIB2DXZY1','irsIB2DXZY1','q2Z7T994QxP','OYYTmYwNydK','OYYTmYwNydK','PztIIL6Q0Gz','PztIIL6Q0Gz','bes6KNWam5o','MgTvQBK3veo','gI6YFdpQ395','Htr3SCNAKlI','xyRPcIc4HpQ','LeMjT11LQr1','QjPvaLk8QEa','i8iyFp92G2p','gf8owYNKJUg','OUQvf88uCoT','NUoNrRXH8wJ','fn0yRfaJ1Ry','BYAFprHpnqm','BbXPVCd1atN','FLSvnHA2AcD','z3gIubAZlbk','O6zNqkyOwY6','FQl1K564eDZ','D9yUZEgVymP','uO9JbDcHTPN','fAOPx1FBThf','QeHsoDFF3Ao','eI2hIQsjM6f','yvo8QaTgLOi','nW8VjjnChEL','sIOb0RmWaB3','bb7gSRcAbMS','narWzuAwKob','UdW4V9TtfGE','zRJdEYMSOb4','Sc4yVMbAIqK','BsuA1PsndFk','FFr8jT1qOVB','BLR6lqf90QO','I2NcQyNXLfj','uURPKPZ9Mps','bAEdWSDuI0h','ABX3Q8zT4sH','SCzmLD3pU7c','ylWFsgvFrk4','eyOSdwOeNtl','HFWTz4oT8XI','JLnK4q4o340','LYoeLiXpz3J','ah4mjKCigND','weq74yjbqTD','R6vxmkLN8F3','axrqueo1SUs','GhU2qFxiZRz','hA9TUYQxW1R','EdEuej32aFi','TRZ8xZW7LFx','GC0OgQgoUPt','tuCSFWz3nnx','pDWMlSqbUyy','JxtFKF5gTG6','mrz4aAO7XvA','R8xXkulZZz3','VetidATY3hF','WiCgqCQcgX','unvAgCJzbpv','D7l437AVZnS','DCenWJOZb0G','r2w3kCHZDGU','r9UlRZ2uddG','b3A5u6KwROj','qwbm3uzywM4','ZcVpw3gMopD','v2rfvP8r2JS','d0DjNlkA2n8','Sjl22ERxwOU','xwKtNfaT5tP','DIf4pAoZ4bj','ynZrnzgUtDj','hVJrK4vUbzP','RwFeEuWUnT7','snuOEngn6fe','T0f3Kch1xCJ','XbJxkenUPrn','Ihcyr6QvFjP','qcdMDarSLFg','tA2ZLd5xlet','lKLkNNG7y94','toyITMXwt4u','vgcmHRUSZsY','kT0V1uUs7fr','yga8iBm1XzS','C1vVJQ8j9cF','VJmc7Q0EqpB','lYRJ2N0eSn9','xgkAahLEaju','J9QHyfgiTXb','YQvlXiGfDUL','QB1aX5LUcYL','OPKENtNmNkv','wmRxu36e5PD','gDCcyYMTQQ4','hiP83bOzetL','zqYgJhRV3wI','sJVWcsJicKJ','L09JvQLW7J2','Z8BOoDgeGcp','XOUPa6blhZ8','TMxjSrhuaRC','sWsRrIGjIbM','iz2vCqvbUBO','rjn5JjJW9MG','vSI37mKGPUm','VTfHzlpJ3AG','HHoQUFDSSSA','wcOR9e5epJt','VyOCthmRCX5','atMuwjEaIf9','CDMCaLnXwFQ','wW86nfp8mY1','OOHWAMjzRlS','Kt5sLeia2oP','DTysAM48PZn','TSEn0nMEDbr','KrhdKPZWNC7','J1fKwObRE6f','qghuslyTwj6','PADUmH9kHRH','GiDNSVDTIaB','x3tlfXl1noB','F983wX6J6AH','gxUaDlJc3sd','w9qXOw1WvqA','PTIHky7dT5R','bbKgQdSx8TX','RQtZRFyU4ga','DZLIkUduB55','OV71SPNyJ0u','z3SVrg7TBXv','WAH1bks5j5G','WdB0Njidae6','bSFccWWM2RE','OG8QHzb8pPk','WMzHM6xjqwe','eWrdsakFFGL','KeB47Le5e8J','q7icSwbpAfO','fumY7pIO83P']
        b_data_element_groups = ['T2otVGuPPiy','m6VDlcFYkmB','qJ95rsHLkNR','noTVip92XJK','fEAVh96L4tJ','dQuMuDJ4I9o','W36Q1h1nuwL','E1bS0BRZ0BM','rXFvMPFixcl','pg5SCl0yv4u','T6aE0zOJAz7','hfdVbT0H1bN','noTVip92XJK','qJ95rsHLkNR','CezsVo5bOob','BBuw8Y7a3we','XXAYWu3oDor','lFgiDcd0Qeg','uP8MaBi1VqV','GBtT9nQgAfl','yAPRoYH2ASm','WH6NPSr1GXL','kFpto9sIRhN']
        # Get Burundi Data
        get_data(b_data_element_ids,b_data_element_groups,b_org_unit_ids,b_conn_identifier,start_date)
        current_run.log_info("== No Burundi")

def get_organisation_units(ou_group_id:str, conn_identifier:str):

    # Open connection to DHIS2
    dhis2_conn = workspace.dhis2_connection(conn_identifier)
    #Get API parameters
    url = f"{dhis2_conn.url}/api/organisationUnitGroups/{ou_group_id}.json?fields=organisationUnits[id]&paging=false"

    # make the API call and parse the response 
    response = requests.get(url, auth=(dhis2_conn.username, dhis2_conn.password))
    json_response = {}
    if response.status_code == 200:
        json_response = json.loads(response.text)
    else:
        print("Error: Failed to fetch organization units with url " + url)

    #Filter OrgUnit ids
    org_unit_ids = []
    for i in json_response['organisationUnits']:
        org_unit_ids.append(i['id'])

    return org_unit_ids

def get_dhis_period(year, mode):
    """ 
    Returns a list of lists of DHIS2 months (YYYYMM -- 202003) based on the year
    specified to the function. For the current year, all months up to N-1 are 
    included in the list. For previous years, the list contains all months.
    """

    current_date = date.today()

    period_start_month = 1
    period_end_month = 12

   
    # current year : up to last month
    # previous years: all months 
    if year == current_date.year:
        period_end_month = current_date.month - 1

    month_list = dhis_month_period_range(year, period_start_month, period_end_month)

    return month_list
                         periods: List[str],
                         org_units: List[str], conn_identifier:str, start_date:str) -> Optional[Dict]:
    """
    Fetch data using dataValueSets API with proper data element group handling.
    Adapted for DHIS2 version 2.39 API.
    """
    try:
        dhis2_conn = workspace.dhis2_connection(conn_identifier)
        current_run.log_info(f"Connected to {dhis2_conn.url}")
        
        params = {
            "paging": "false", 
            "includeDeleted": "false"
        }

        # Handle date range parameters for 2.39 API
        if start_date != "2010-01-01":
            end_date = date.today()
            formatted_end_date = end_date.strftime("%Y-%m-%d")
            params["startDate"] = start_date
            params["endDate"] = formatted_end_date

        # Add periods, org units, and data elements as comma-separated values
        # DHIS2 2.39 API expects these as single parameters with comma-separated values
        if periods:
            params["period"] = periods  # DHIS2 2.39 uses semicolon for multiple periods
        if org_units:
            params["orgUnit"] = org_units  # DHIS2 2.39 uses semicolon for multiple org units
        if data_element_ids:
            params["dataElement"] = data_element_ids  # DHIS2 2.39 uses semicolon for multiple data elements

        current_run.log_info(f"Reading data from {dhis2_conn.url}")
        current_run.log_info(f"API Parameters: {params}")
        query_string = urlencode(params, doseq=True)
        current_run.log_info(f"Param string: {query_string}")
        current_run.log_info(f"URL : {dhis2_conn.url}/api/dataValueSets?{query_string}")
        try:
            response = requests.get(
                f"{dhis2_conn.url}/api/dataValueSets?{query_string}",
                params={"query": query_string},
                auth=HTTPBasicAuth(dhis2_conn.username, dhis2_conn.password),
                timeout=400
            )

            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError:
                    print("Invalid JSON response. Content:")
                    print(response.text[:500])
                return None
    
            print(f"Error {response.status_code}: {response.text[:200]}")
            if response.status_code in [401, 403]:
                return None

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {str(e)}")
        
    return None

def fetch_data_value_sets(data_element_ids: List[str],
                         periods: List[str],
                         org_units: List[str], conn_identifier:str, start_date:str) -> Optional[Dict]:
    """
    Fetch data using dataValueSets API with proper data element group handling.
    """
    try:
        dhis2_conn = workspace.dhis2_connection(conn_identifier)
        current_run.log_info(f"Connected to {dhis2_conn.url}")
        params_list =[]
        if start_date == "2010-01-01":
            params = {
                "paging": "false", 
                "includeDeleted": "false"
            }

            params_list = [(k, v) for k, v in params.items()]
            
            params_list.extend([("period", pe) for pe in periods])
        else:
            endDate = date.today()
            formatedEndDate = endDate.strftime("%Y-%m-%d")
            params = {
                "paging": "false", 
                "includeDeleted": "false",
                "startDate": start_date,
                "endDate": formatedEndDate
            }

            params_list = [(k, v) for k, v in params.items()]

        # Add each org unit, dataelement as a separate parameter
        params_list.extend([("orgUnit", ou) for ou in org_units])
        params_list.extend([("dataElement", de) for de in data_element_ids])
        print(params_list)
        current_run.log_info(f"Reading data from {dhis2_conn.url}")

        try:
            response = requests.get(
                f"{dhis2_conn.url}/api/dataValueSets",
                params=params,
                auth=HTTPBasicAuth(dhis2_conn.username, dhis2_conn.password),
                timeout=400
            )

            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError:
                    print("Invalid JSON response. Content:")
                    print(response.text[:500])
                return None
    
            print(f"Error {response.status_code}: {response.text[:200]}")
            if response.status_code in [401, 403]:
                return None

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {str(e)}")
        
    
    return None  

def dhis_month_period_range(year, start, end):
    r = [
        f'{year}{str(x).zfill(2)}' for x in 
        range(start, end + 1)
    ]

    return r

@lifenet___hmiss.task
def get_data(
    data_elements:str,
    data_element_groups:str,
    org_units:str, 
    conn_identifier:str,
    start_date:str
):
    try:
        # set periods to every previous 4 months
        today = date.today()
        first = today.replace(day=1)
        last_month = first - timedelta(days=1)
        first_of_last_month = last_month.replace(day=1)
        last_2_month = first_of_last_month - timedelta(days=1)
        last_month_3 = last_2_month - timedelta(days=31)    
        last_month_4 = last_month_3 - timedelta(days=31)    
        last_month_5 = last_month_4 - timedelta(days=31)    
        last_month_6 = last_month_5 - timedelta(days=31)
        periods = [last_month_6.strftime("%Y%m"),last_month_5.strftime("%Y%m"),last_month_4.strftime("%Y%m"),last_month_3.strftime("%Y%m"), last_2_month.strftime("%Y%m"), last_month.strftime("%Y%m")]
        print(periods)

        # Fetch data for the last 6 month if start date is not specified (or its default value 2010-01-01)
        data = fetch_data_value_sets(data_elements, periods, org_units, conn_identifier, start_date)
        
        #Convert to pandas
        if data and "dataValues" in data:
            df = pd.DataFrame(data["dataValues"])
            # Continue with your processing
        else:
            print("No data returned from API or missing 'dataValues' key")
            # Handle the empty case appropriately
            df = pd.DataFrame()  # Create empty DataFrame

        print(df.head(1))
        columns_to_drop =['created','followup']
        data_f = df.loc[df.apply(lambda row: any(item in data_elements for item in row), axis=1)]
        data_f.drop(columns=[col for col in columns_to_drop if col in df.columns], axis=1)
        data_f = data_f.replace('NhSoXUMPK2K','HllvX50cXC0')
        data_f = data_f.replace('Joer6DI3Xaf','HllvX50cXC0')
        data_f = data_f.replace('Tt7fU5lUhAU','HllvX50cXC0')
        data_f = data_f.replace('GKkUPluq2QJ','HllvX50cXC0')

        # Split the dataframe into batches
        batch_size = 5000
        for i in range(0, len(data_f), batch_size):
            batch = data_f.iloc[i:i+batch_size]
                    
            # Create a list of datavalues
            data_values = []
            for index, row in batch.iterrows():
                value = {'dataElement': row["dataElement"], 
                    'period': row["period"],
                    'orgUnit': row["orgUnit"],
                    'categoryOptionCombo': row["categoryOptionCombo"],
                    'value': row["value"]
                    }
                data_values.append(value)

            #Open connection to lifenet server
            prod_dhis2_conn = workspace.dhis2_connection("lifenet")
            values = {
                'dataValues': data_values
            }

            #Push values 
            response = requests.post(
                f'{prod_dhis2_conn.url}/api/dataValueSets',
                auth=(prod_dhis2_conn.username, prod_dhis2_conn.password),
                headers={'Content-Type': 'application/json'},
                data=json.dumps(values, allow_nan=True)
            )

            current_run.log_info(f"{response.text} - to - {conn_identifier}")
    except Exception as e:
        print('Error! on {conn_identifier} Code: {c}, Message, {m}'.format(c = type(e).__name__, m = str(e)))

    return response


if __name__ == "__main__":
    lifenet___hmiss()
