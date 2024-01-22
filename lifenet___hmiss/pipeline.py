import os
import shutil
import requests
import json
import polars as pl
import pandas as pd

from datetime import date, timedelta

from openhexa.sdk import workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.sdk import workspace, pipeline


@pipeline("lifenet---hmiss", name="Lifenet - HMISs")
def lifenet___hmiss():
    #Lifenet Connection Identifier
    life_identifier = "lifenet"

    #Kenya parameters
    k_conn_identifier = "kenya-hmis"
    k_org_unit_group_id = "maMqwNj9gNl"
    k_org_unit_ids = get_organisation_units(k_org_unit_group_id, life_identifier)
    k_data_element_ids = ["h14HsnrOBCE","p6QtFjN5M1R","ovJmoErpCRY","ijYBkFVUTMV","qMYm7fGXkXP","rfmxXpWFwMq","hGhUdN3KgrK","ZI3qZ2mfBER","V6AhoeTTClY","B4jH9l9ZRKt","OvDwPPIrEnB","BmWgF3GJgIs","D923dTnerdk","lVNKL5UQ8Sb","FwsTJpdxT4C","qrTk1w6Y5Bk","idiiaWIh2qi","ekh0oJidJ38","f9vesk5d4IY","etbuJ3chcZL","Fz0LzxMT1vV","hxXwYIgT8rI","f9vesk5d4IY","y6Vi9uqabo4","xNW6Y9Kitok","k8qXy4LyyJj","ovJmoErpCRY","ijYBkFVUTMV","Kx64gGqaFVq","sMqM8DwiAaj","jaPrPmor6WV","rAZBTMa7Jy3","UqKC1DJnymn","rYn8RCcHJg1","wZtDYVBa41h","GAr6xu6f1n7","KDFtI6rqYwI","BYMSIbnUzXQ","AC1Iorxdijc","dPRCstLVkZu","UiGlnkNmCut","UsyFvMBxvn0","KuMX8VqCejs","Gp38RIEAOCb","Gw8C0uUQade","pp5FTNgUXmf","wxg6cuH9cXF","VT8eVxvtphF","mdNncoz3lFe","fJ7CyT97iJC","MVIKFGNvNJp","B2Pfs8oAuiy","YD9L08SeDPE","kfuGsBdD3rp","qcKLKy25k4G","O3qECFGrzeF","QYUkPEhnhBX","tKGNp1722Hc","KH4eOYS7USz","MI3SdIsA0iU","gw4Zog5qvBy","ku80YRejJPg","K1Zzhafeukq","OyddF1qflzP","g1tOm44qfsi","Ba7pFRVTVnC","TLOb9t8hfAp","IEl87u0hwfZ","sGqznpDvcmt","zVjl7nB15Kx","sRbqc5gBQhI","Jw3alL29ZEi","VuXfcCV7RtR","tBPGQFhWB7M","dncP9CPM8QS","dlldM4hP2Wk","xAAEwRrU8EH","BkzYqWdGikV","dncP9CPM8QS"]
    k_data_element_groups = "n0fE9qoi5N1,qADpr4dQZuC,JMNedIzqWGA,Tco6NWSjw53,XUyBl4L2raq,VWCIP8wQvMc,dTUuUMw64bk,SWuN3c3i8fL,wqLiG4JdCcO,VbAGjhNkaOw,XIhYn40pCwk,DCnQwGyolzD,gT7SBAw0EMU,ZEly6FddL4n"

    # Get Kenya Data
    get_data(k_data_element_ids,k_data_element_groups,k_org_unit_ids,k_conn_identifier)

    #Uganda parameters
    u_conn_identifier = "uganda-hmis"
    u_org_unit_group_id = "rP6kTiTZ3SD"
    u_org_unit_ids = get_organisation_units(u_org_unit_group_id, life_identifier)
    u_data_element_ids = ["SFMipMUafSk","idXOxt69W0e","MxAg9De4cra","NYs9Ct3X4FD","eEllJg5kQn6","G4uZN2Y4oAG","Quc9uIBIxk6","HLyPaHaoHSu","ehj9x0CXNN5","Ys31ug5E3f1","z1s4aIzf8ga","ujs4ipzA4tb","NbG01i1Md55","dmSXPXTnV0e","QHawVF72X6E","rDZlacXXUPM","QFolEgl8SEB","WAjgHQVxVVm","yctOQDdXZav","sv6SeKroHPV","sQ4EexvvhVe","UwnR5kr982Y","PaceRdSpmgy","dt0srLW7MmK","A3r248imZ1L","dyfJZ2Llr6F","gsl7Ws1pTg1","","NXCJqj7Edfo","ZVKD2XRg6Ab","Fat3y7a43qY","iOdD0RgSRoD","O3dDxoUjSqo","fCj5K4Xqy7T","BsyuYs7db5P","pGndkat5h9s","jSQRBvez5Pq","zhTC6HT9No1","VHi82Mcd77w","ibfLXaygcNS","HnSiI2XX2Sy","qSe7OiDB18A","Onpo1O1A7jr","bkCFVYMFMed","jNpa9MJhPZm","lUH7AWlsExq","LJ8MLXHctGs","RYcEItpNCUp","T8W0wbzErSF","ULL9lX3DO7V","hrTskGHP0Av","WAdw7P2USJV","fEz9wGsA6YU","nnmOsAUssg9","h5qpxtCVwAp","pWZcOnKaoCa","ZUpS5SUz7Zf","NiFpmskP3EW","Q9nSogNmKPt","RnLOFSYaAhp","tsJBUFW2Vjm","hEvz6VY4COC","JOWj87d62MK","qCXtnqhwKak","GwQD5HqHGIv","IbiK1GS74qv","HkI0xDwvvr8","XwydjcQlZOI","KzBNR8ia9ot","xzZZOy6cmr8","XXZZbU4B2N3","Tf0xH5TZftt","tfQukLqcJz9"]
    u_data_element_groups = "a8zBKLRQJqX,qpIxOxGdXTh,Zo9TmJGnZRe,vPvLSIPrfnb,W1NQYn9zeT0,NMFTigF3Xkt,Q0c1uezRn4g"

    # Get Uganda Data
    get_data(u_data_element_ids,u_data_element_groups,u_org_unit_ids,u_conn_identifier)

    #Ghana parameters
    g_conn_identifier = "ghana-hmis"
    g_org_unit_group_id = "rX8bkQ79b7i"
    g_org_unit_ids = get_organisation_units(g_org_unit_group_id, life_identifier)
    g_data_element_ids = ["glWIq7TGk1T","SiJyJ4JCQd7","CFcb6iFTvms","hIDhWbrli42","TW9czUWSlP7","oEcZpxk7Rf3","iWoeSCakQaE","TnMryyPjk6T","rvAykfczHRi","KypKeCZLDgR","HRloFZqfG5B","iPXFJfsJDTd","hHDud7w6330","g2KgXPM3ipk","Z993NkZima8","MFioLhQXlXt","A6kORjQ3fbc","HsUHBWG0t4M","QrL0nT8aBIZ","GGJC8yHTgG0","LlDVZFo4fKF","Yj3CvImgye6","WSEAfuLyxMZ","r3gJ6naFz44","wXZTi4HU1UU","tAs56Gg7OIN","ErXpaxK9G4E","E4TOOJajLta","M30fe90zqP5","IPZ5slSg5WQ","xcXQtQ0SEhm","S5fCNtU9Rx0","BkPLgZ6332Q","jdxvRz2ER79","bRs0F1NfmDG","RbKUJAe04DP","l9ypN2TOXS1","SzYUXUum9hD","w5bWyShTRda","kExPsDMDVVA","Uokq3G5dVBp","XrL5bKOgej8","xrB0tfWxbsm","NycE4uys6hY","C1dNDN3VFAV","w6VDsTH0wW9","T40BcJkQHtj","PPgBRKOI44q","wwEkOuMluUq","UmyD6nF6NHG","KcuB4aMv9gJ","gvI1z5wjd4W","cIDN3Z84HHR","H4riF3Fdl4E","MdQj9sVKTrw","WPsmra5FomD"]
    g_data_element_groups = "CrbRhxAjC7I,BKV7si1tSyb,Y5bBv4wOO54,zuwpJUFpWPq,bKnH5cjibju,l7x4Th8Yyjk,ao8xuAkvB43,e6vgHQeJ3T1,wFAWlGYTcX4,YGpjoeCmLnW,oI1dhPjSGiu,kWEUURm4mdL,bKnH5cjibju,BvSZct8R9nK,AfN57jhF423,RzLaF1jmWeZ"

    # Get Ghana Data
    get_data(g_data_element_ids,g_data_element_groups,g_org_unit_ids,g_conn_identifier)

    #Malawi parameters
    m_conn_identifier = "malawi-hmis"
    m_org_unit_group_id = "XyfPVCdqOwo"
    m_org_unit_ids = get_organisation_units(m_org_unit_group_id, life_identifier)
    m_data_element_ids = ["Hcho5dCr","ynY0QLNPFpn","ftyQGpirFHE","lM48Ysgzz0H","p7SpeubB59p","QD9JBJ6ZakN","o8fLTxlRPzv","JYYgpjhp0qN","Pni0DP4zlWJ","BIZneYMhHku","iBfokcDFZnI","ueZ9XGbE7Dn","us7AbR44NDw","YGHvR4UKMwB","F1DkHBIW7tO","BjOKMvEcpXM","VEgce5WnxHV","Kp3yd5v3rec","lPvOHlySRwG","x5RtjHfY0gI","wRnWFfvI3E8","yCJBWuhuOLc","kz8lbDDDz34","NZzRHJ6tmPE","MoximPvXuj6","l2GWTPGK4QP","bPvw8ZdBgRe","EJ0qYm1FrcH","YlydKJIyGFF","jz793Wzon1W","bjLZY1FqdDJ","QiIfrKC5Ox8","XgYKomvdkBo","xgW1P5I9cN2","NeEMZqYtG4c","NxggSy8gzqo","DQNn56buEd5","hfihixkbjhh","bycjbqrbtoz","qphfxmgmvug","e5ETCXCmBaf","QQoLFyarP03","OvDhfIIJPS7","jxaojehpgby","dqmDYv3P8Bi","PfNXkoOulIo","zwCJtAIraUB","JpGmb8ofLAv","JtnAK5uZU4Q","pfuJANCCQOc","ueC8Esnjd2M","Lwav7YQBgjC","vcEWpcFjEuU","BtjzKJIuTJv","Y2vzWp3rPce","NtkuSvtY8D0","TAsjIcQVYG6","eoS9HXn97nF","bYV6cz5L9Kz","KOu770F7YOQ","hPhTbuLFnx4","YIP4fvYcGYQ","dTK0MlakKzU","yOOBXy8nGXF","hqAy9jJ6gmT","CKuA0GiXFMS","xnuL680PHT4","jrdZDuwSrzc","mHSfE7GArgh","QvexDntI3mf","v2I6dWgLYrM","QVMyrWnXwYU","TSCmQaQwcDx","KiwtHd7nWCX","PkrtOx1olaV","mraYTWoViTj","SVC5nKhvcQr","ZgDrKbIPkgE","Jtrq0pYsqT0","Jpkh8pokAY7","EUvtRZSLh2F","Z73ODYforG3"]
    m_data_element_groups = "l2cbutKQU9K,FnOzKaLnGy1,TJ7XX2vEtyd,SLDYafSzSrj,xiMFKdTWRFp,tsefi6doleb,AnGLAlOrzpN,vJxPpUXNNci,sl3HwATctcg,eeWCk7FjSKE,HRWNG10u4OR,FrXg1nHZxL2,kkHokihKMp2,t2MyS9B4IoV,mQ9GjwjE9wM,IGEQm1wP1dJ,RHgqynGGPy1"

    # Get Malawi Data
    get_data(m_data_element_ids,m_data_element_groups,m_org_unit_ids,m_conn_identifier)
 
    #DRC parameters
    d_conn_identifier = "drc-hmis"
    d_org_unit_group_id = "K74ZpyPIZEI"
    d_org_unit_ids = get_organisation_units(d_org_unit_group_id, life_identifier)
    d_data_element_ids = ["TTK7nV64EvX","DqiMxAlXTOe","AxJhIi7tUam","aK0QXqm8Zxn","rfeqp2kdOGi","hoPZUCWGbvD","BjzoMcHdu1k","e8kkhUinpNJ","kEUOeuRvDWk","zLeY0EViRHg","UxD03qX5O0t","NJw6RhEcKIS","gtsulcE7ena","kGmdIbd3mbn","NiNF1GtNTkd","j5VRZFJzUli","OLme8tEPGov","MuIfWlqJ0Cf","aoIpDzKttLJ","O0eYKOFhMee","gDnbubwzuts","bSINVpDbI6c","QRs9kVKS047","AjklQZlDGEp","j2DKCdQwurw","i8LtPxBnqnV","n2k6Sbtwtgh","I1x8R4StgSA","L0GHHIlZDIq","zcH4sipFcUr","qFbtP6YBqkJ","bOfjRt0MPZn","SzhsGuHCYEL","Jr1CDZh5BKh","a8YKnKG0JXZ","ELYjn1wkdnI","UgI0jQybW1l","zrsma7SQkbW","LAFIsrZWPof","uKxLt4S9xUP","W25tOXS0rxS","f1MiAAIu366","j3P9bSnwF12","ozVdkud4F03","SPGaDDw2JzG","I6tLH3nk9tw","bzD4QxaNkJm","eoIUv4bRBOk","jqAIZmVwkZd","smQqPyAxDiJ","ImmKmjpMW6u","MzeK7PxCDSM","sUVL3kym5uR","Q2hgxeRH7uX","O4zM5PA27NE","gihu8MW6ga6","xnB0V4LRCoA","WOm7ro9YniO","EhshL3PqSAh","muGZqMdLcJz","N60LF19AcTf","p3HvbM5WquI","WlTa2LjlF8q","Jcb2h160gb0","cPAApegfYAZ","PuTEbadhAPf","i5zmivDIHN8","wwde2QtlIRN","cTLKwfG8pSv","J8R3WFpMAZI","WGu60o5mePq","CgR8kWNdK3W","WwxqaHSeiwd","YYKXOc7xBUi","uNdFg1eymsa","YBnkF8oFyBO","M2JQW0H44dI","Y7zTdjUfSlz","hxTYMhz2fYf","e1hDWsq2olK","jEJ1LzB0BLZ","Z86bHthxwVG","v7DDmc4zsWu","nNYSsMt45hw","LspDmk116zD"]
    d_data_element_groups = "pRGFD7tsGz2,LgPxtZ1JSjH,e2G7cfoNZ4G,qCFRiss9p9v,lxb21pmYdqw,xJCY73ee2vN,tDKaxgERUAR,WytfXnKt73I,wmzXaVyWASM,B2l6jh9FYgh,fZx86mbqEv3,VXjXo78YHz4,E3nAmNYoFzM,aPYWzIrR4Rh,zoYXWaxdkCi,GewECwrQK4t,LTuiwzK2XYO,o0C4iIwXeJc,xFF02gBEhPR,EUEZWxAmccl,d3R2MH1m0RI,JG9Vqun6FzO,fkVmBwwOIeo,PAk8ACdsgp3"

    # Get DRC Data
    get_data(d_data_element_ids,d_data_element_groups,d_org_unit_ids,d_conn_identifier)

    #Burundi parameters
    b_conn_identifier = "burundi-hmis"
    b_org_unit_group_id = "e6KX5E5Oe9c"
    b_org_unit_ids = get_organisation_units(b_org_unit_group_id, life_identifier)
    b_data_element_ids = ["h9EytkY6M2z","h9EytkY6M2z","Kg32Hb5UpUC","Kg32Hb5UpUC","vp9olxQ1dv9","S8RjnQDBObs","GyT9slNFY8D","ghN2d6CroMJ","BXl88CcTZHO","GyT9slNFY8D","ghN2d6CroMJ","BXl88CcTZHO","cgGBTu6Xm65","kuTbYtSy3Pp","CJi5bewIsNN","SKit63CLTux","bJ75Lbz0Qss","J5fW63Jsd04","CZwF141N3gX","RYYCIBCP8Pe","JJmxhoIxByc","SFTpvzIq0P5","e1abJ5ezYzK","Y3RQAtpnJ3O.rqQuGU2IAs6","Y0alfPyZ81U","NYd6KIjGS3Z","emF0ll8UN2T","OES4dSgIBxW","ur0FrVtmhPG","SDm3VVG1jMp","sc8fs4GfcOy","OEy7p30izz1","DCldsat9Igd","M0bYQyYeYMQ","lMQRe6ez8rM","qm29orau9fC","JarzwzBpEgo","EDEIzAre9gR","niBkwVXCx3q","Z0l9LkFuuym","YXQhcmUdPFb","tmxhqBiX1hC","fqItdPDbDoo","fqItdPDbDoo","sHUojx0LgOV","xIDB6prYVYw","hqw08UsCoEn","hqw08UsCoEn","irsIB2DXZY1","irsIB2DXZY1","q2Z7T994QxP","OYYTmYwNydK","OYYTmYwNydK","PztIIL6Q0Gz","PztIIL6Q0Gz","bes6KNWam5o","MgTvQBK3veo","MgTvQBK3veo","gI6YFdpQ395","Htr3SCNAKlI","xyRPcIc4HpQ","LeMjT11LQr1","QjPvaLk8QEa","i8iyFp92G2p","gf8owYNKJUg","OUQvf88uCoT","NUoNrRXH8wJ","fn0yRfaJ1Ry","BYAFprHpnqm","BbXPVCd1atN","FLSvnHA2AcD","z3gIubAZlbk","O6zNqkyOwY6","FQl1K564eDZ","D9yUZEgVymP","uO9JbDcHTPN","fAOPx1FBThf","QeHsoDFF3Ao","eI2hIQsjM6f","yvo8QaTgLOi","nW8VjjnChEL","sIOb0RmWaB3","bb7gSRcAbMS","narWzuAwKob","UdW4V9TtfGE","zRJdEYMSOb4","Sc4yVMbAIqK","BsuA1PsndFk","FFr8jT1qOVB","BLR6lqf90QO","I2NcQyNXLfj","uURPKPZ9Mps","bAEdWSDuI0h","ABX3Q8zT4sH","SCzmLD3pU7c","ylWFsgvFrk4","eyOSdwOeNtl","HFWTz4oT8XI","JLnK4q4o340","LYoeLiXpz3J","ah4mjKCigND","weq74yjbqTD","R6vxmkLN8F3","axrqueo1SUs","GhU2qFxiZRz","hA9TUYQxW1R","EdEuej32aFi","TRZ8xZW7LFx","GC0OgQgoUPt","tuCSFWz3nnx","pDWMlSqbUyy","JxtFKF5gTG6","mrz4aAO7XvA","R8xXkulZZz3","VetidATY3hF","WiCgqCQcgX","unvAgCJzbpv","D7l437AVZnS","DCenWJOZb0G","r2w3kCHZDGU","r9UlRZ2uddG","b3A5u6KwROj","qwbm3uzywM4","ZcVpw3gMopD","v2rfvP8r2JS","d0DjNlkA2n8","Sjl22ERxwOU","xwKtNfaT5tP","DIf4pAoZ4bj","ynZrnzgUtDj","hVJrK4vUbzP","RwFeEuWUnT7","snuOEngn6fe","T0f3Kch1xCJ","XbJxkenUPrn","Ihcyr6QvFjP","qcdMDarSLFg","tA2ZLd5xlet","lKLkNNG7y94","toyITMXwt4u","vgcmHRUSZsY","kT0V1uUs7fr","yga8iBm1XzS","C1vVJQ8j9cF","VJmc7Q0EqpB","lYRJ2N0eSn9","xgkAahLEaju","J9QHyfgiTXb","YQvlXiGfDUL","QB1aX5LUcYL","OPKENtNmNkv","wmRxu36e5PD","gDCcyYMTQQ4","hiP83bOzetL","zqYgJhRV3wI","sJVWcsJicKJ","L09JvQLW7J2","Z8BOoDgeGcp","XOUPa6blhZ8","TMxjSrhuaRC","sWsRrIGjIbM","iz2vCqvbUBO","rjn5JjJW9MG","vSI37mKGPUm","VTfHzlpJ3AG","HHoQUFDSSSA","wcOR9e5epJt","VyOCthmRCX5","atMuwjEaIf9","CDMCaLnXwFQ","wW86nfp8mY1","OOHWAMjzRlS","Kt5sLeia2oP","DTysAM48PZn","TSEn0nMEDbr","KrhdKPZWNC7","J1fKwObRE6f","qghuslyTwj6","PADUmH9kHRH","GiDNSVDTIaB","x3tlfXl1noB","F983wX6J6AH","gxUaDlJc3sd","w9qXOw1WvqA","PTIHky7dT5R","bbKgQdSx8TX","RQtZRFyU4ga","DZLIkUduB55","OV71SPNyJ0u","z3SVrg7TBXv","WAH1bks5j5G","WdB0Njidae6","bSFccWWM2RE","OG8QHzb8pPk","WMzHM6xjqwe","eWrdsakFFGL","KeB47Le5e8J","q7icSwbpAfO","fumY7pIO83P"]
    b_data_element_groups = "E1bS0BRZ0BM,rXFvMPFixcl,pg5SCl0yv4u,T6aE0zOJAz7,hfdVbT0H1bN,noTVip92XJK,qJ95rsHLkNR,CezsVo5bOob,BBuw8Y7a3we,XXAYWu3oDor,lFgiDcd0Qeg,uP8MaBi1VqV,GBtT9nQgAfl,yAPRoYH2ASm,WH6NPSr1GXL,kFpto9sIRhN"

    # Get Burundi Data
    get_data(b_data_element_ids,b_data_element_groups,b_org_unit_ids,b_conn_identifier)


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


@lifenet___hmiss.task
def get_data(
    data_elements:str,
    data_element_groups:str,
    org_units:str, 
    conn_identifier:str
):
    # set periods
    today = date.today()
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)
    first_of_last_month = last_month.replace(day=1)
    last_2_month = first_of_last_month - timedelta(days=1)
    periods = [last_2_month.strftime("%Y%m"), last_month.strftime("%Y%m")]
    dhis2_conn = workspace.dhis2_connection(conn_identifier)
    dhis = DHIS2(dhis2_conn, cache_dir=".cache")
    data = dhis.data_value_sets.get(
        data_element_groups=data_element_groups,
        org_units=org_units,
        periods=periods
        )
    #Convert to pandas
    df = pl.DataFrame(data)

    #data_values = df[df['dataElement']].isin(data_elements)
    data_f = df.to_pandas().loc[df.to_pandas().apply(lambda x: x.dataElement in data_elements, axis=1)]
    data_f.drop(['created','followup'], axis=1)
    data_f['categoryOptionCombo'] = data_f['categoryOptionCombo'].replace('NhSoXUMPK2K','HllvX50cXC0')
    data_f['categoryOptionCombo'] = data_f['categoryOptionCombo'].replace('Joer6DI3Xaf','HllvX50cXC0')
    data_f['categoryOptionCombo'] = data_f['categoryOptionCombo'].replace('Tt7fU5lUhAU','HllvX50cXC0')
    data_f['categoryOptionCombo'] = data_f['categoryOptionCombo'].replace('GKkUPluq2QJ','HllvX50cXC0')

    # Split the dataframe into batches
    batch_size = 500
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

        #Open connection to test server
        prod_dhis2_conn = workspace.dhis2_connection("lifenet")
        values = {
            'dataValues': data_values
        }

        #Push values 
        print(values)
        response = requests.post(
            f'{prod_dhis2_conn.url}/api/dataValueSets',
            auth=(prod_dhis2_conn.username, prod_dhis2_conn.password),
            headers={'Content-Type': 'application/json'},
            data=json.dumps(values, allow_nan=True)
        )

        print("\n")
        print(response.text)
        print("\n")
        print(">Batch Done<")
    return response


if __name__ == "__main__":
    lifenet___hmiss()
