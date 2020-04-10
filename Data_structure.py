import nltk
import pymssql
import pandas as pd
import boto3
import json
import re

class DataStructure(object):
    #将数据从数据库读出
    def Read_data(self):
        with pymssql.connect('.', 'sa', '19950804', 'MeadHeadLine', autocommit=True) as conn:
            sql = 'select Id,TextEn from PatientStory where Id = 392'
            dfindex = pd.read_sql(sql=sql, con=conn)
            try:
                ArticleId = dfindex["Id"][0]
                data = dfindex["TextEn"][0]
                #切割数据
                # print(type(ArticleId),ArticleId,data)
                self.sensplit(data,ArticleId)
            except IndexError:
                print('未找到')
            except Exception as e:
                print('出错',e)

    #切割句子
    def sensplit(self,data,ArticleId):
        tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
        result_sentence = tokenizer.tokenize(data)
        # print(result_sentence)
        result_split = '\n'.join(result_sentence)
        #将切割好的句子存取
        #定义一个变量，用于记录句子所处位置
        offset_num = 0
        #记录每个句子的内容，起始位置，结束位置
        data_list = []
        for i in range(len(result_sentence)):
            text = result_sentence[i]
            data_dict = {'ArticleId':str(int(ArticleId)),'sentence':text,'begin':offset_num,'end':offset_num+len(text)}
            offset_num += len(text)
            data_list.append(data_dict)
        data_sentence = pd.DataFrame(data_list)
        #调用亚马逊接口识别
        # self.med_api(result_sentence,data_sentence)
        #提取候选医药实体
        # self.selectMentity(ArticleId,data_sentence)
        #利用识别的数据属性匹配病例
        self.splitandorder(data_sentence,ArticleId)

    #利用亚马逊识别实体，入库
    def med_api(self,result_sentence,data_sentence):
        comprehend = boto3.client(service_name='comprehend', region_name='us-west-2')
        resultEid = 0  # 普通实体
        resultKid = 0  # 关键词
        resultSid = 0  # 语法
        resultMId = 0  # 医疗实体
        for i in range(len(result_sentence)):
            text = data_sentence['sentence'][i]
            ArticleId = data_sentence['ArticleId'][i]
            begin = data_sentence['begin'][i]
            print(begin)
            end = data_sentence['end'][i]

            #识别实体
            result = comprehend.detect_entities(Text=text,LanguageCode='en')
            Entities = result['Entities']
            for entity in Entities:
                try:
                    with pymssql.connect('.', 'sa', '19950804', 'lijinhui', autocommit=True) as conn:
                        with conn.cursor(as_dict=True) as cursor:
                            sql1 = "insert into AWSEntities ([ArticalId],[resultId],[BeginOffset],[EndOffset],[Score],[Text],[Type])values(%d,%d,%d,%d,%f,'%s','%s')"
                            cursor.execute(sql1 % (int(ArticleId), resultEid, entity['BeginOffset'] + begin,
                                                   entity['EndOffset'] + begin, entity['Score'],
                                                   entity['Text'].replace("'", "''"),
                                                   entity['Type'].replace("'", "''")))

                    resultEid = resultEid + 1
                except Exception as ex:
                    print('普通实体识别出错： ' + str(ex))

            # #识别关键字
            result = comprehend.detect_key_phrases(Text=text, LanguageCode='en')
            entities = result["KeyPhrases"]
            for entity in entities:
                try:
                    with pymssql.connect('.', 'sa', '19950804', 'lijinhui', autocommit=True) as conn:
                        with conn.cursor(as_dict=True) as cursor:
                            sql1 = "insert into AWSKeyPhrase ([ArticalId],[resultId],[BeginOffset],[EndOffset],[Score],[Text])values(%d,%d,%d,%d,%f,'%s')"
                            cursor.execute(sql1 % (int(ArticleId), resultKid, entity['BeginOffset'] + begin,
                                                   entity['EndOffset'] + begin, entity['Score'],
                                                   entity['Text'].replace("'", "''")))

                    resultKid = resultKid + 1
                except Exception as ex:
                    print('关键词识别出错： ' + str(ex))

            #识别语法
            result = comprehend.detect_syntax(Text=text, LanguageCode='en')
            entities = result['SyntaxTokens']
            for entity in entities:
                try:
                    with pymssql.connect('.', 'sa', '19950804', 'lijinhui', autocommit=True) as conn:
                        with conn.cursor(as_dict=True) as cursor:
                            sql1 = "insert into AWSSyntax ([ArticalId],[resultId],[BeginOffset],[EndOffset],[Score],[Tag],[Text],[TokenId])values(%d,%d,%d,%d,%f,'%s','%s',%d)"
                            cursor.execute(sql1 % (int(ArticleId), resultSid, entity['BeginOffset'] + begin,
                                                   entity['EndOffset'] + begin,
                                                   entity['PartOfSpeech']['Score'], entity['PartOfSpeech']['Tag'],
                                                   entity['Text'].replace("'", "''"), entity['TokenId']))

                    resultSid = resultSid + 1
                except Exception as ex:
                    print('语法识别出错： ' + str(ex))

            #识别医药实体
            client = boto3.client(service_name='comprehendmedical', region_name='us-west-2')
            result = client.detect_entities(Text=text)
            entities = result['Entities']

            for entity in entities:
                try:
                    with pymssql.connect('.', 'sa', '19950804', 'lijinhui', autocommit=True) as conn:
                        with conn.cursor(as_dict=True) as cursor:
                            sql1 = "insert into AWSmedical ([ArticalId],[resultId],[BeginOffset],[EndOffset],[Score],[Text],[Category],[Type],[Traits])values(%d,%d,%d,%d,%f,'%s','%s','%s','%s')"
                            cursor.execute(sql1 % (int(ArticleId), resultMId, entity['BeginOffset'] + begin,
                                                   entity['EndOffset'] + begin, entity['Score'],
                                                   entity['Text'].replace("'", "''"),
                                                   entity['Category'].replace("'", "''"),
                                                   entity['Type'].replace("'", "''"),
                                                   str(entity['Traits']).replace("'", "''")))

                    resultMId = resultMId + 1

                    if 'Attributes' in entity:
                        for each in entity['Attributes']:
                            with pymssql.connect('.', 'sa', '19950804', 'lijinhui', autocommit=True) as conn:
                                with conn.cursor(as_dict=True) as cursor:
                                    sql1 = "insert into AWSmedical ([ArticalId],[resultId],[BeginOffset],[EndOffset],[Score],[Text],[Category],[Type],[Traits],[AttributesOrNot])values(%d,%d,%d,%d,%f,'%s','%s','%s','%s',1)"
                                    cursor.execute(sql1 % (
                                    int(ArticleId), resultMId, each['BeginOffset'] + begin,
                                    each['EndOffset'] + begin, each['Score'], each['Text'].replace("'", "''"),
                                    entity['Category'].replace("'", "''"), each['Type'].replace("'", "''"),
                                    str(each['Traits']).replace("'", "''")))
                                    resultMId = resultMId + 1
                except Exception as ex:
                    print('医疗实体识别出错： ' + str(ex))
            print('*'*50)


        # comprehend = boto3.client(service_name='comprehend', region_name='us-west-2')
        # print('Calling DetectDominantLanguage')
        # print(json.dumps(comprehend.detect_dominant_language(Text=result_split), sort_keys=True, indent=4))
        # print("End of DetectDominantLanguage\n")
        #
        # print('Calling DetectEntities')
        # print(json.dumps(comprehend.detect_entities(Text=result_split, LanguageCode='en'), sort_keys=True, indent=4))
        # print('End of DetectEntities\n')

    #利用识别的数据属性匹配病例
    def splitandorder(self,data_sentence,ArticleId):
        #普通实体数据
        sql_cmd = "select * from [AWSEntities] where ArticalId = %d" % ArticleId
        with pymssql.connect('.', 'sa', '19950804', 'lijinhui', autocommit=True) as conn:
            dfAE = pd.read_sql(sql=sql_cmd, con=conn)

        #关键字数据
        sql_cmd = "select * from [AWSKeyPhrase] where ArticalId = %d" % ArticleId
        with pymssql.connect('.', 'sa', '19950804', 'lijinhui', autocommit=True) as conn:
            dfASP = pd.read_sql(sql=sql_cmd, con=conn)

        #语法数据
        sql_cmd = "select * from [AWSSyntax] where ArticalId = %d" % ArticleId
        with pymssql.connect('.', 'sa', '19950804', 'lijinhui', autocommit=True) as conn:
            dfAS = pd.read_sql(sql=sql_cmd, con=conn)

        #医药实体数据
        sql_cmd = "select * from [AWSmedical] where ArticalId = %d" % ArticleId
        with pymssql.connect('.', 'sa', '19950804', 'lijinhui', autocommit=True) as conn:
            dfAM = pd.read_sql(sql=sql_cmd, con=conn)

        # 按句子循环
        # 根据位置筛选所有在该句子里面的实体和关键词
        # 从普通实体表中分析是否有时间,date类型或QUANTITY类型，days,months,years结尾
        # 先处理医疗实体信息，判断类型，同时搜索 关键词中的包含该医疗实体的词，替换为空剩下的词可能为aws不能识别的词，记下。

        # ANATOMY,MEDICAL_CONDITION,MEDICATION,PROTECTED_HEALTH_INFORMATION,TEST_TREATMENT_PROCEDURE
        EntityTime = ""
        # 后续关键词应该是字母或数字开始和结束
        regkw = '[a-zA-Z0-9]+[\s\S]*[a-zA-Z0-9]+'

        # 个人信息表
        PHIinfo = pd.DataFrame(
            columns=["PID", "NAME", "Gender", "AGE", "ADDRESS", "PROFESSION", "EMAIL", "PHONE_OR_FAX", "DATE",
                     "URL"])
        # 治疗信息表
        TreatStructTable = pd.DataFrame(
            columns=["TreatMethod", "GENERIC_NAME", "BRAND_NAME", "MEDICAL_STATUS", "MEDICAL_FORM", "ROUTE_OR_MODE",
                     "MEDICAL_CYCLE", "MEDICAL_DOSE", "MEDICAL_FREQ", "MEDICAL_EFFECT", "MEDICAL_EFFECTTIME",
                     "MEDICAL_SIDEEFFECT", "MEDICAL_ACCEPT", "MEDICAL_STOPREASON", "BeginTime", "EndTime",
                     "Location", "MEDICAL_STRENGTH", "EntityTime"])
        # 诊断信息表
        DiagnoseStructTable = pd.DataFrame(
            columns=["CancerCate", "CancerType", "CancerStage", "FamilyHis", "DiagnosisLocation", "DiagnosisDoctor",
                     "EntityTime"])
        # 检查信息表
        CheckStructTable = pd.DataFrame(
            columns=["TEST_TIME", "TEST_NAME", "TEST_VALUE", "TEST_UNIT", "SYSTEM_ORGAN_SITE", "EntityTime"])

        PHIlist = []  # 患者信息
        dict = {"PID": "", "NAME": "", "Gender": "", "AGE": "", "ADDRESS": "", "PROFESSION": "", "EMAIL": "",
                "PHONE_OR_FAX": "", "DATE": "", "URL": "", "HEIGHT": "", "WEIGHT": "", "Marital": "",
                "Fertility": ""}
        PHIlist.append(dict)
        PHIdict = {"PID": "档案编号", "NAME": "姓名", "Gender": "性别", "AGE": "年龄", "ADDRESS": "地址", "PROFESSION": "职业",
                   "EMAIL": "电子邮箱", "PHONE_OR_FAX": "电话传真", "DATE": "日期", "URL": "网址", "HEIGHT": "身高",
                   "WEIGHT": "体重", "Marital": "婚姻状况", "Fertility": "婚育状况"}
        TSTlist = []  # 治疗
        dict = {"TreatMethod": "", "GENERIC_NAME": "", "BRAND_NAME": "", "MEDICAL_STATUS": "", "MEDICAL_FORM": "",
                "ROUTE_OR_MODE": "", "MEDICAL_CYCLE": "", "MEDICAL_DOSE": "", "MEDICAL_FREQ": "",
                "MEDICAL_EFFECT": "", "MEDICAL_EFFECTTIME": "", "MEDICAL_SIDEEFFECT": "", "MEDICAL_ACCEPT": "",
                "MEDICAL_STOPREASON": "", "BeginTime": "", "EndTime": "", "Location": "", "MEDICAL_STRENGTH": "",
                "EntityTime": ""}
        TSTlist.append(dict)
        TSTdict = {"TreatMethod": "治疗方式", "GENERIC_NAME": "药名", "BRAND_NAME": "商品名", "MEDICAL_STATUS": "治疗状态",
                   "MEDICAL_FORM": "治疗形式", "ROUTE_OR_MODE": "服用方式", "MEDICAL_CYCLE": "", "MEDICAL_DOSE": "剂量",
                   "MEDICAL_FREQ": "频率", "MEDICAL_EFFECT": "治疗效果", "MEDICAL_EFFECTTIME": "有效时长",
                   "MEDICAL_SIDEEFFECT": "副作用", "MEDICAL_ACCEPT": "接受程度", "MEDICAL_STOPREASON": "停药原因",
                   "BeginTime": "开始时间", "EndTime": "结束时间", "Location": "治疗地点", "MEDICAL_STRENGTH": "药物强度",
                   "EntityTime": "实体时间"}
        DSTlist = []  # 诊断
        dict = {"CancerCate": "", "CancerType": "", "CancerStage": "", "FamilyHis": "", "DiagnosisLocation": "",
                "DiagnosisDoctor": "", "SIGN": "", "EntityTime": ''}
        DSTlist.append(dict)
        DSTdict = {"CancerCate": "肿瘤类别", "CancerType": "分型", "CancerStage": "分期", "FamilyHis": "家族史",
                   "DiagnosisLocation": "诊断地点", "DiagnosisDoctor": "诊断医生", "SIGN": "症状", "EntityTime": "实体时间"}
        CSTlist = []  # 检查
        dict = {"TEST_TIME": "", "TEST_NAME": "", "TEST_VALUE": "", "TEST_UNIT": "", "SYSTEM_ORGAN_SITE": "",
                "EntityTime": '', "TEST_DIRECTION": "", "TEST_SIZE": "", "TEST_CHARACTER": "",
                "TEST_DEGREE": "", "TEST_PHYSICAL": ""}
        CSTlist.append(dict)
        dict = {"TEST_TIME": "检查时间", "TEST_NAME": "检查项目", "TEST_VALUE": "检查值", "TEST_UNIT": "检查单位",
                "SYSTEM_ORGAN_SITE": "身体器官", "EntityTime": "实体时间", "TEST_DIRECTION": "方位", "TEST_SIZE": "大小",
                "TEST_CHARACTER": "性状", "TEST_DEGREE": "程度", "TEST_PHYSICAL": "查体"}

        #循环读出句子表中的句子的开始和结束位置
        for j in range(len(data_sentence)):
            begin = data_sentence["begin"][j]
            end = data_sentence["end"][j]

            #将数据库读出的实体与该句子位置匹配
            #普通实体
            dfAEsub = dfAE[(dfAE['BeginOffset'] >= begin) & (dfAE['EndOffset'] <= end) & (
                        (dfAE['Type'] == 'QUANTITY') | (dfAE['Type'] == 'DATE'))]
            #关键字
            dfASPsub = dfASP[(dfASP['BeginOffset'] >= begin) & (dfASP['EndOffset'] <= end)]
            #语法
            dfASsub = dfAS[(dfAS['BeginOffset'] >= begin) & (dfAS['EndOffset'] <= end)]
            #医药实体
            dfAMsub = dfAM[(dfAM['BeginOffset'] >= begin) & (dfAM['EndOffset'] <= end)]

            # #从实体中获取时间
            for k in range(len(dfAEsub)):
                entitytext = str.lower(dfAEsub.iloc[k]['Text'])
                if dfAEsub.iloc[k]['Type'] == 'DATE' or entitytext.endswith('days') or entitytext.endswith(
                        'months') or entitytext.endswith('years'):
                    EntityTime = dfAEsub.iloc[k]['Text']

            # 亚马逊识别的医学信息
            for k in range(len(dfAMsub)):
                try:
                    if dfAMsub.iloc[k]['Category'] == 'ANATOMY' and dfAMsub.iloc[k][
                        'Type'] == 'SYSTEM_ORGAN_SITE':
                        CSTlist = self.listdeal(CSTlist, "SYSTEM_ORGAN_SITE", dfAMsub.iloc[k]['Text'],
                                           EntityTime)

                    elif dfAMsub.iloc[k]['Category'] == 'MEDICAL_CONDITION':
                        if dfAMsub.iloc[k]['Traits'] != []:
                            if 'DIAGNOSIS' in dfAMsub.iloc[k]['Traits']:
                                DSTlist = self.listdeal(DSTlist, "CancerCate", dfAMsub.iloc[k]['Text'],
                                                   EntityTime)
                            if 'SYMPTOM' in dfAMsub.iloc[k]['Traits']:
                                TSTlist = self.listdeal(TSTlist, "MEDICAL_SIDEEFFECT", dfAMsub.iloc[k]['Text'],
                                                   EntityTime)
                            if 'SIGN' in dfAMsub.iloc[k]['Traits']:
                                DSTlist = self.listdeal(DSTlist, "SIGN", dfAMsub.iloc[k]['Text'], EntityTime)

                    elif dfAMsub.iloc[k]['Category'] == 'MEDICATION':
                        # 要处理一下药物，有些化疗药物跟手术治疗放到一下，所以当是MEDICATION的时候，应该搜索最近的TREATMENT_NAME和PROCEDURE_NAME，如果与当前的不一致，则新建一条记录
                        #treatText为判断出的治疗方案类型
                        treatText = self.nearlyText(dfAMsub, dfAMsub.iloc[k]['BeginOffset'],
                                               dfAMsub.iloc[k]['EndOffset'], ['TEST_TREATMENT_PROCEDURE'],
                                               ['PROCEDURE_NAME', 'TREATMENT_NAME'])
                        # 靠距离判断还是会有误差，最好改为判断药物的类型，是化疗药还是靶向药等
                        TSTlist = self.listdeal(TSTlist, dfAMsub.iloc[k]['Type'], dfAMsub.iloc[k]['Text'],
                                           EntityTime, 'TreatMethod', treatText)

                    elif dfAMsub.iloc[k]['Category'] == 'PROTECTED_HEALTH_INFORMATION':
                        # 注意DATE是否单独处理
                        if dfAMsub.iloc[k]['Type'] == 'ID':
                            PHIlist = self.listdeal(PHIlist, "PID", dfAMsub.iloc[k]['Text'], EntityTime)
                        elif dfAMsub.iloc[k]['Type'] == 'NAME' and PHIlist[len(PHIlist) - 1]['NAME'] == \
                                dfAMsub.iloc[k]['Text']:
                            pass
                        else:
                            PHIlist = self.listdeal(PHIlist, dfAMsub.iloc[k]['Type'], dfAMsub.iloc[k]['Text'],
                                               EntityTime)

                    elif dfAMsub.iloc[k]['Category'] == 'TEST_TREATMENT_PROCEDURE':
                        if dfAMsub.iloc[k]['Type'] == 'PROCEDURE_NAME':
                            TSTlist = self.listdeal(TSTlist, "TreatMethod", dfAMsub.iloc[k]['Text'], EntityTime)
                        elif dfAMsub.iloc[k]['Type'] == 'TREATMENT_NAME':
                            TSTlist = self.listdeal(TSTlist, "TreatMethod", dfAMsub.iloc[k]['Text'], EntityTime)
                        else:
                            CSTlist = self.listdeal(CSTlist, dfAMsub.iloc[k]['Type'], dfAMsub.iloc[k]['Text'],
                                               EntityTime)

                except Exception as ex:
                    print('实体 ' + str(ex))

            # 关键词识别自主的医学实体
            for l in range(len(dfASPsub)):
                #遍历关键字表
                KeyWords = str.lower(dfASPsub.iloc[l]['Text'].strip())
                try:
                    resultkw = re.findall(regkw, KeyWords)
                    if resultkw != []:
                        # QYword = resultkw[0].replace('and', '').replace('or', '').replace('  ', ' ').strip()
                        #对匹配的字符去掉停止词
                        resultkw_res = resultkw[0].strip().split(' ')
                        for i in range(len(resultkw_res) - 1, -1, -1):
                            print(resultkw_res[i])
                            if resultkw_res[i] == 'or' or resultkw_res[i] == 'and':
                                resultkw_res.remove(resultkw_res[i])
                            if resultkw_res[i] == '':
                                resultkw_res.remove(resultkw_res[i])
                        QYword=' '.join(resultkw_res)
                        with pymssql.connect('.', 'sa', '19950804', 'lijinhui',
                                             autocommit=True) as msscon:
                            with msscon.cursor(as_dict=True) as cursor:
                                cursor.execute(
                                    "select top 1 * from  [dbo].[QYMedKeyWord]   where CHARINDEX(lower([Text]),'%s' ) <> 0 order by len(Text) desc " % QYword)
                                result = cursor.fetchone()
                                # 需要保证找到的是一个单词，而不是几个字母片段
                                subwords = QYword.split(' ')
                                if (result is not None) and (
                                        (' ' in result['Text'] and str.lower(result['Text']) in QYword) or (
                                        result['Text'] in subwords)):
                                    if result['Category'] == 'CheckStructTable':
                                        CSTlist = self.listdeal(CSTlist, result['Type'], result['Text'],
                                                           EntityTime)
                                    elif result['Category'] == 'DiagnoseStructTable':
                                        DSTlist = self.listdeal(DSTlist, result['Type'], result['Text'],
                                                           EntityTime)
                                    elif result['Category'] == 'TreatStructTable':
                                        if result['Type'] == "MEDICAL_EFFECT":
                                            # 要处理治疗效果，应该搜索最近的TREATMENT_NAME和PROCEDURE_NAME，如果与当前的不一致，则新建一条记录
                                            treatText = self.nearlyText(dfAMsub, dfASPsub.iloc[l]['BeginOffset'],
                                                                   dfASPsub.iloc[l]['EndOffset'],
                                                                   ['TEST_TREATMENT_PROCEDURE'],
                                                                   ['PROCEDURE_NAME', 'TREATMENT_NAME'])

                                            TSTlist = self.listdeal(TSTlist, result['Type'], result['Text'],
                                                               EntityTime, 'TreatMethod', treatText, 1)
                                        else:
                                            TSTlist = self.listdeal(TSTlist, result['Type'], result['Text'],
                                                               EntityTime)
                                    elif result['Category'] == 'PHIinfo':
                                        PHIlist = self.listdeal(PHIlist, result['Type'], result['Text'],
                                                           EntityTime)
                except Exception as ex:
                    print('启元关键词错误 ' + str(ex))

            # 普通实体识别自主的医学实体
            for l in range(len(dfAEsub)):
                KeyWords = str.lower(dfAEsub.iloc[l]['Text'].strip())
                inAE = 0
                for k in range(len(dfAMsub)):
                    if inAE == 1:
                        break
                    try:
                        MedEntity = str.lower(dfAMsub.iloc[k]['Text'].strip())
                        QYword = KeyWords.replace(MedEntity, "").strip()
                        if KeyWords.startswith(MedEntity) or KeyWords.endswith(MedEntity) and len(
                                QYword) > 1:
                            resultkw = re.findall(regkw, QYword)
                            if resultkw != []:
                                resultkw_res = resultkw[0].strip().split(' ')
                                for i in range(len(resultkw_res) - 1, -1, -1):
                                    print(resultkw_res[i])
                                    if resultkw_res[i] == 'or' or resultkw_res[i] == 'and':
                                        resultkw_res.remove(resultkw_res[i])
                                    if resultkw_res[i] == '':
                                        resultkw_res.remove(resultkw_res[i])
                                QYword = ' '.join(resultkw_res)
                                inAE = 1
                        with pymssql.connect('.', 'sa', '19950804', 'MeadHeadLine',
                                             autocommit=True) as msscon:
                            with msscon.cursor(as_dict=True) as cursor:
                                # 包含这个词最长的QY实体
                                cursor.execute(
                                    "select top 1 * from  [dbo].[QYMedKeyWord]   where lower(Text) = '%s'  " % QYword)
                                result = cursor.fetchone()
                                if result is not None:
                                    inAE = 1
                                    if result['Category'] == 'CheckStructTable':
                                        CSTlist = self.listdeal(CSTlist, result['Type'], result['Text'],
                                                           EntityTime)
                                    elif result['Category'] == 'DiagnoseStructTable':
                                        DSTlist = self.listdeal(DSTlist, result['Type'], result['Text'],
                                                           EntityTime)
                                    elif result['Category'] == 'TreatStructTable':
                                        TSTlist = self.listdeal(TSTlist, result['Type'], result['Text'],
                                                           EntityTime)
                                    elif result['Category'] == 'PHIinfo':
                                        PHIlist = self.listdeal(PHIlist, result['Type'], result['Text'],
                                                           EntityTime)

                    except Exception as ex:
                        print('启元关键词错误 ' + str(ex))

        ff = open('F:/down/cancer/Entity.txt', 'w')
        print("完成一句")
        ff.write("输出个人信息：\n")
        for each in PHIlist:
            for key in each:
                if each[key] != "":
                    ff.write(key + " : " + each[key] + "\n")
            ff.write("\n")

        ff.write("输出诊断信息：\n")
        for each in DSTlist:
            for key in each:
                if each[key] != "":
                    ff.write(key + " : " + each[key] + "\n")
            ff.write("\n")

        ff.write("输出治疗信息：\n")
        for each in TSTlist:
            for key in each:
                if each[key] != "":
                    ff.write(key + " : " + each[key] + "\n")
            ff.write("\n")

        ff.write("输出检验信息：\n")
        for each in CSTlist:
            for key in each:
                if each[key] != "":
                    ff.write(key + " : " + each[key] + "\n")
            ff.write("\n")

    #将匹配的数据插入对应的列表
    def listdeal(self,inlist,column,value,EntityTime, keycol = '',keyvalue = '',seq = 0):
        try:
            if inlist[len(inlist) - 1][column] == "":
                if keycol == "" or (keycol != "" and (
                        inlist[len(inlist) - 1][keycol] == '' or inlist[len(inlist) - 1][keycol] == keyvalue)):
                    inlist[len(inlist) - 1][column] = value
                else:
                    if seq == 1:
                        # 从后往前遍历
                        for i in range(len(inlist) - 1, -1, -1):
                            if inlist[i][keycol] == keyvalue:
                                inlist[i][column] = value
                                break
                    else:
                        bdict = inlist[0].copy()
                        for key in bdict:
                            bdict[key] = ""
                        bdict[column] = value
                        bdict["EntityTime"] = EntityTime
                        inlist.append(bdict)
            else:
                bdict = inlist[0].copy()
                for key in bdict:
                    bdict[key] = ""
                bdict[column] = value
                bdict["EntityTime"] = EntityTime
                inlist.append(bdict)
        except Exception as ex:
            print('listdeal处理list错误 ' + str(ex))
        finally:
            return inlist

    #根据位置判断药物是属于哪个治疗方案
    def nearlyText(self,df,beginoff,endoff,cate,type):
        dis = 10000
        Text = ''
        for i in range(len(df)):
            if df.iloc[i]['Category'] in cate and df.iloc[i]['Type'] in type:
                if df.iloc[i]['BeginOffset'] <= beginoff:
                    disCal = beginoff-df.iloc[i]['BeginOffset']
                    if disCal < dis:
                        Text = df.iloc[i]['Text']
                        dis = disCal
                elif df.iloc[i]['EndOffset'] >= endoff:
                    disCal = df.iloc[i]['BeginOffset'] - endoff
                    if disCal < dis:
                        Text = df.iloc[i]['Text']
                        dis = disCal
        return Text


    #提取候选医疗实体
    def selectMentity(self,ArticalId,data_sentence):
        sql_cmd = "select * from [AWSEntities] where ArticalId = %d" % ArticalId
        with pymssql.connect('.', 'sa', '19950804', 'lijinhui', autocommit=True) as conn:
            dfAE = pd.read_sql(sql=sql_cmd, con=conn)

        sql_cmd = "select * from [AWSKeyPhrase] where ArticalId = %d" % ArticalId
        with pymssql.connect('.', 'sa', '19950804', 'lijinhui', autocommit=True) as conn:
            dfASP = pd.read_sql(sql=sql_cmd, con=conn)

        sql_cmd = "select * from [AWSSyntax] where ArticalId = %d" % ArticalId
        with pymssql.connect('.', 'sa', '19950804', 'lijinhui', autocommit=True) as conn:
            dfAS = pd.read_sql(sql=sql_cmd, con=conn)

        sql_cmd = "select * from [AWSmedical] where ArticalId = %d" % ArticalId
        with pymssql.connect('.', 'sa', '19950804', 'lijinhui', autocommit=True) as conn:
            dfAM = pd.read_sql(sql=sql_cmd, con=conn)

        dfindex = []
        sql_cmd = "select * from PatientStory where Id = %d" % ArticalId
        with pymssql.connect('.', 'sa', '19950804', 'MeadHeadLine', autocommit=True) as conn:
            dfindex = pd.read_sql(sql=sql_cmd, con=conn)


        #循环读出句子的位置，将此句子中未在医药实体中识别出的实体匹配出并入库
        for i in range(len(data_sentence)):
            be = data_sentence['begin'][i]
            end = data_sentence['end'][i]

            #将在此范围内的普通实体取出
            dfAEsub = dfAE[(dfAE['BeginOffset'] >= be) & (dfAE['EndOffset'] <= end)& (
                        (dfAE['Type'] == 'QUANTITY') | (dfAE['Type'] == 'DATE'))]
            dfASPsub = dfASP[(dfASP['BeginOffset'] >= be) & (dfASP['EndOffset'] <= end)]
            dfASsub = dfAS[(dfAS['BeginOffset'] >= be) & (dfAS['EndOffset'] <= end)]
            dfAMsub = dfAM[(dfAM['BeginOffset'] >= be) & (dfAM['EndOffset'] <= end)]
            #正则匹配关键词
            regkw = '[a-zA-Z0-9]+[\s\S]*[a-zA-Z0-9]+'


            for l in range(len(dfAEsub)):
                inAE = 0
                KeyWords = dfAEsub.iloc[l]['Text'].strip()
                for k in range(len(dfAMsub)):
                    try:
                        MedEntity = dfAMsub.iloc[k]['Text'].strip()
                        QYword = KeyWords.replace(MedEntity, "").strip()
                        if MedEntity in KeyWords and len(QYword) > 1:
                            inAE = 1
                            resultkw = re.findall(regkw, QYword)
                            if resultkw != []:
                                QYword = resultkw[0].replace('and', '').replace('or', '').replace('  ', ' ').strip()
                                if len(QYword) > 1:
                                    # 先看看这个分离出来的词是否属于这个句子已识别出来的实体
                                    if QYword not in dfAMsub['Text'].values:
                                        with pymssql.connect('.', 'sa', '19950804', 'lijinhui',
                                                             autocommit=True) as msscon:
                                            with msscon.cursor(as_dict=True) as cursor:
                                                #查看QYmedicalKeyWord表中是否已存在此关键词
                                                cursor.execute(
                                                    "select top 1 Id from  [dbo].[QYmedicalKeyWord]  where Text = '%s' " % QYword)
                                                result = cursor.fetchone()
                                                if result is None:
                                                    effect_row = cursor.execute(
                                                        "insert into [dbo].[QYmedicalKeyWord](Text,KeyWords,MedEntity,Category,Type) values ('%s','%s','%s','%s','%s')" % (
                                                            QYword, KeyWords, MedEntity,dfAMsub.iloc[k]['Category'],dfAMsub.iloc[k]['Type']))

                            pass
                    except Exception as ex:
                        print('启元关键词错误 ' + str(ex))

                if inAE == 0:  # 如果没有实体在这个关键词中，直接放入候选表
                    with pymssql.connect('.', 'sa', '19950804', 'lijinhui', autocommit=True) as msscon:
                        with msscon.cursor(as_dict=True) as cursor:
                            if KeyWords not in dfAMsub['Text'].values:
                                cursor.execute(
                                    "select top 1 Id from  [dbo].[QYmedicalKeyWord]  where Text = '%s' " % KeyWords.replace(
                                        "'", "''"))
                                result = cursor.fetchone()
                                if result is None:
                                    cursor.execute(
                                        "select top 1 * from  [dbo].[AWSmedical]  where Text = '%s' " % KeyWords.replace(
                                            "'", "''"))
                                    result = cursor.fetchone()
                                    if result is None:
                                        effect_row = cursor.execute(
                                            "insert into [dbo].[QYmedicalKeyWord](Text,KeyWords,Category) values ('%s','%s','%s')" % (
                                                KeyWords.replace("'", "''"),
                                                KeyWords.replace("'", "''"),
                                                dfAEsub.iloc[l]['Type'].replace("'", "''")))
                                    else:
                                        effect_row = cursor.execute(
                                            "insert into [dbo].[QYmedicalKeyWord](Text,Category,Type) values ('%s','%s','%s')" % (
                                                KeyWords.replace("'", "''"), result['Category'].replace("'", "''"),
                                                result['Type'].replace("'", "''")))


            for l in range(len(dfASPsub)):
                KeyWords = dfASPsub.iloc[l]['Text'].strip()
                for k in range(len(dfAMsub)):
                    try:
                        MedEntity = dfAMsub.iloc[k]['Text'].strip()
                        QYword = KeyWords.replace(MedEntity, "").strip()
                        if MedEntity in KeyWords and len(QYword) > 1:
                            resultkw = re.findall(regkw, QYword)
                            if resultkw != []:
                                QYword = resultkw[0].replace(' and', '').replace(' or', '').replace('and ', '').replace(
                                    'or ', '').strip()
                                if len(QYword) > 1:
                                    # 先看看这个分离出来的词是否属于这个句子已识别出来的实体
                                    if QYword not in dfAMsub['Text'].values:
                                        with pymssql.connect('.', 'sa', '19950804', 'lijinhui',
                                                             autocommit=True) as msscon:
                                            with msscon.cursor(as_dict=True) as cursor:
                                                cursor.execute(
                                                    "select  Id from  [dbo].[QYmedicalKeyWord]  where Text = '%s' " % QYword)
                                                result = cursor.fetchone()
                                                if result is None:
                                                    effect_row = cursor.execute(
                                                        "insert into [dbo].[QYmedicalKeyWord](Text,KeyWords,MedEntity) values ('%s','%s','%s')" % (
                                                        QYword, KeyWords, MedEntity))
                                pass
                    except Exception as ex:
                        print('启元关键词错误 ' + str(ex))





if __name__=='__main__':
    Data = DataStructure()
    Data.Read_data()