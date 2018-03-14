# -*- coding: utf-8 -*-‘
##作者：刻刻团队 卢月亮    时间：2018年3月2号  联系方式：luyueliang423@163.com
import MySQLdb
import jieba
import warnings
warnings.filterwarnings(action='ignore', category=UserWarning, module='gensim')
import gensim
from gensim import corpora
import time
#建立与数据库的连接
conn1=MySQLdb.connect(host='localhost'      #主机名
                      ,port=3308            #端口
                      ,user='root'          #用户名
                      ,passwd='123456'       #密码
                      ,db='test'         #数据库名
                      ,charset='utf8')
 #也可以简写成conn1=MySQLdb.connect('localhost','root','admin','test123')
#  #创建游标
###########################################################
#查询teacherdata 中所有学院的种类
Institution=conn1.cursor()
Institution.execute('select institution from teacherdata group by institution')
TotalInstitution=Institution.fetchall()
Institution.close()
###########################################################
#根据学院种类筛选老师
TeacherID=conn1.cursor()
TeacherID.execute('select id,institution from teacherdata')
TotalTeacherID=TeacherID.fetchall()
StartNumber=0
TotalAuthorID=[]
SeletAuthorID=[word for word in TotalTeacherID]
##########################################
# SingleAuthorID = [ID for ID in SeletAuthorID if ID[1] in TotalInstitution[0][0]]
# for i in SingleAuthorID:
#     print  i[0]#测试是否获得教师的ID
##########################################
for Teacher in TotalInstitution:
    SingleAuthorID = [ID for ID in SeletAuthorID if ID[1] in Teacher[0]]
    TotalAuthorID.append(SingleAuthorID)
TeacherID.close()
############################################################
#根据教师ID查找老师的文献和文献的ID
ControlNumber=0
for AuthorID in TotalAuthorID:
    ###################
    # #单个院系测试
    # if ControlNumber>1:
    #     continue
    # ControlNumber = ControlNumber + 1
    #print len(AuthorID)
    ###################
    TotalPaper=[]
    PaperID=conn1.cursor()
    for ID in AuthorID:#一个院系有多个AuthorID，每一个AuthorID里面有多篇论文，每篇论文收集摘要关键字和论文id，所以生成的数据是三维的数据
        #ID代表authorID院系中的每个老师的
        PaperID.execute('select id,abstract,keyword from paper where author_id =%d'%ID[0])
        Institution=ID[1]
        TotalPaperID=PaperID.fetchall()
        if len(TotalPaperID)<1:#当前老师没有论文则跳过不记录
            continue
        for paper in TotalPaperID:
            TotalPaper.append(paper)
        #print len(TotalPaperID)#每个老师有多少篇论文
        #print len(TotalPaper)#整个院系有多少论文
    PaperID.close()
    ####################################以上数据源确定为SQL#############################################################
    print Institution
    if u'中国近现代史研究所'in Institution:
        ControlNumber+=1
        print 'iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii'
    if ControlNumber==0:
        continue
    ####################正文#####################################
    DictionaryWords=[]#利用关键词生成自定义结巴词典
    for LLines in TotalPaper:
        #print LLines[2]
        Lines=LLines[2].strip('\n')
        Lines=Lines.strip('\t')
        Lines=Lines.split(',')
        ClearNumber=0
        for word in Lines:
            if len(word)==0:
                continue
            jieba.add_word('%s'%word)#将关键字加入结巴词典
    print Institution
    print ('词典更新完成。正在分词......')
    #################################################################
    DocWord=[]
    stopwords = {}.fromkeys([line.rstrip() for line in open('StopWords.txt')])
    KeywordsWeight=30#关键词所占的权重
    AddKeywords=0
    for LLines in TotalPaper:#开始生成文本，并分词
        #print LLines[1]
        Lines = LLines[1].strip('\n')
        Lines = Lines.strip('\t')
        Lines = Lines.strip('')
        if Lines<=1:
            Lines=LLines[2]
        if len(LLines[2])<=1:
            Lines = Lines
        else:
            KeyWordLines = LLines[2].strip('\n')
            KeyWordLines = KeyWordLines.strip('\t')
            KeyWordLines = KeyWordLines.strip(' ')
            Lines=Lines+KeywordsWeight*KeyWordLines
        Lines=Lines.strip('\n')
        Lines = Lines.strip('\t')
        Lines = Lines.strip(' ')
        segs=jieba.cut(Lines)
        segs = [word.encode('utf-8') for word in list(segs)]
        segs = [word for word in list(segs) if word not in stopwords]
        DocWord.append(segs)
    print ('分词完成。正在生成文本向量.......')
#########################################################################

    dictionary = corpora.Dictionary(DocWord)#生成向量词典
    corpus = [dictionary.doc2bow(text) for text in DocWord]
    ###################################
    if len(corpus)==0:#如果整个院系的论文是0，那么跳过院系
        continue
    ###################################
    ######选取少量文章测试程序#########
    #corpus=corpus[:200]
    ###################################

    print ('已生成训练集文本向量。正在进行模型训练......')
    # generate LDA model
    print ('本院系文章总数为%d，即将分为主题数50个，关键字20个......'%len(corpus))
    time1 = time.time()
    #time.sleep(15)
    ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics=50, id2word=dictionary, passes=50)
    result=ldamodel.print_topics(num_topics=50, num_words=20)
   # print result[1]
    time2 = time.time()
    print '模型训练用时：',time2 - time1
    print ('LDA模型训练完成。正在输出结果......')

###############################################操作完成，以下是写入数据库###############################################
    doc_lda = ldamodel[corpus]
    # ResultData=[]#一共需要4列数据，paperid & institution & paper_topic & word_topic
    SelectWord=0
    print ('正在链接数据库......')
    for Topic in doc_lda:
        # SingleResult = []
        # SingleResult.append(int(TotalPaper[SelectWord][0]))
        # SingleResult.append(Institution)
        # SingleResult.append(int(Topic[0][0]))
        wordTopic=[i[1] for i in result if int(Topic[0][0])==i[0]]
        # SingleResult.append(wordTopic)

        # ResultData.append(SingleResult)
    #print ResultData
        resultdata = conn1.cursor()
        resultdata.execute("insert into lda values(%d,'%s',%d,'%s')"%(int(TotalPaper[SelectWord][0]), Institution.encode('utf-8'),int(Topic[0][0]), wordTopic[0].encode('utf-8')))
        resultdata.close()
        SelectWord += 1
    conn1.commit()
conn1.close()#断开数据库连接
# #############################################################
# #####测试数据来源本地excel，测试完成后更改为数据库来源#######
# fr=open('paper1.txt')
# LinesAbstract = fr.readlines()#摘要
# fr.close()
# fr=open('paper.txt')
# LinesKeyWords = fr.readlines()#关键字
# fr.close()
# #############################################################
# ####################正文#####################################
# DictionaryWords=[]#利用关键词生成自定义结巴词典
# for Lines in LinesKeyWords:
#     Lines=Lines.strip('\n')
#     Lines=Lines.strip('\t')
#     Lines=Lines.split(',')
#     ClearNumber=0
#     for word in Lines:
#         if len(word)==0:
#             continue
#         jieba.add_word('%s'%word)#将关键字加入结巴词典
# #################################################################
# DocWord=[]
# stopwords = {}.fromkeys([line.rstrip() for line in open('StopWords.txt')])
# KeywordsWeight=30#关键词所占的权重
# AddKeywords=0
# for Lines in LinesAbstract:#开始生成文本，并分词
#    # Lines = Lines.strip('\n')
#    # Lines = Lines.strip('\t')
#    # Lines = Lines.strip('')
#     if Lines<=1:
#         Lines=LinesKeyWords[AddKeywords]
#     if len(LinesKeyWords[AddKeywords])<=1:
#         Lines = Lines
#     else:
#         LinesKeyWords[AddKeywords]=LinesKeyWords[AddKeywords].strip('\n')
#         LinesKeyWords[AddKeywords] = LinesKeyWords[AddKeywords].strip('\t')
#         LinesKeyWords[AddKeywords] = LinesKeyWords[AddKeywords].strip(' ')
#         Lines=Lines+KeywordsWeight*LinesKeyWords[AddKeywords]
#
#     AddKeywords +=1
#     Lines=Lines.strip('\n')
#     Lines = Lines.strip('\t')
#     Lines = Lines.strip(' ')
#     segs=jieba.cut(Lines)
#     segs = [word.encode('utf-8') for word in list(segs)]
#     segs = [word for word in list(segs) if word not in stopwords]
#     DocWord.append(segs)
# #########################################################################
# dictionary = corpora.Dictionary(DocWord)#生成向量词典
# corpus = [dictionary.doc2bow(text) for text in DocWord]
# # generate LDA model
# ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics=20, id2word=dictionary, passes=20)
# result=ldamodel.print_topics(num_topics=20, num_words=10)
#
# #ldamodel.get_document_topics(bow, minimum_probability=None, minimum_phi_value=None, per_word_topics=False)#per_word_topics为False时获取某个文档最有可能具有的主题列表，per_word_topics为True时还得到文档中每个词属于的主题列表，bow为文档，
# # 打印前20个topic的词分布
# #模型的保存/ 加载
# # ldamodel.save('zhwiki_lda.model')
# # ldamodel = models.ldamodel.LdaModel.load('zhwiki_lda.model')
#
#
# # dictionary = corpora.Dictionary(DocWord[1])#生成向量词典
# # corpus = [dictionary.doc2bow(text) for text in DocWord[1]]
# # test_doc = DocWord[1] #新文档进行分词
# # doc_bow = dictionary.doc2bow(test_doc)      #文档转换成bow
# doc_lda = ldamodel[corpus]                  #得到新文档的主题分布
# #输出新文档的主题分布
# print doc_lda[2]
# # for topic in doc_lda:
# #     print "%s\t%f\n"%(ldamodel.print_topic(topic[0]), topic[1])
# # print result
# for re in result:
#     for rre in re:
#         print rre