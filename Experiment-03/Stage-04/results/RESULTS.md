# 阶段4结果及实现过程

## 预处理test.csv和train.csv数据集

### 预处理测试集test.csv数据
1. 把这test.csv数据集里label字段表示-1值剔除掉，保留需要预测的数据
2. 假设需要预测的数据中label字段均为1

进入Terminal，运行下列代码：
> $ cd ~/dbtaobao/dataset // 进入存储测试集文件夹 </br> $ vim preprocess_test.sh // 建立新脚本文件来预处理数据

preprocess_test.sh详细代码：
> #!/bin/bash </br> 
// 提供第一个参数作为输入文件 </br> 
inputfile=$1 </br> 
// 提供第二个参数作为输出文件 </br>
outputfile=$2 </br> </br> 
awk -F “,” ‘BEGIN{ </br> 
id=0; </br> 
} </br> 
{ </br> 
if($1 && $2 && $3 && $4 && !$5){ </br> 
id=id+1; </br> 
print $1″,”$2″,”$3″,”$4″,”1 </br> 
if(id==10000){ </br> 
exit </br> 
} </br> 
} </br> 
}’ $infile > $outfile </br> 

接下来执行preprocess_test.sh，并将结果命名为test_after.csv，命令如下：
> $ chmod +x ./preprocess_test.sh </br> $ ./preprocess_test.sh ./test.csv ./test_after.csv

### 预处理测试集train.csv数据
方法类似
1. 删除第一行字段名称
2. 剔除掉train.csv中字段值部分字段值为空的数据

删除第一行Shell命令：
> $ sed -i '1d' train.csv

建立新脚本文件 preprocess_train.sh 来预处理数据：
> $ cd ~/dbtaobao/dataset // 进入存储测试集文件夹 </br> $ vim preprocess_train.sh // 建立新脚本文件来预处理数据

类似测试集，preprocess_train.sh详细代码：
> #!/bin/bash </br> 
// 提供第一个参数作为输入文件 </br> 
inputfile=$1 </br> 
// 提供第二个参数作为输出文件 </br>
outputfile=$2 </br> </br> 
awk -F “,” ‘BEGIN{ </br> 
id=0; </br> 
} </br> 
{ </br> 
if($1 && $2 && $3 && $4 && ($5!=-1)){ </br> 
id=id+1; </br> 
print $1″,”$2″,”$3″,”$4″,”$5 </br> 
if(id==10000){ </br> 
exit </br> 
} </br> 
} </br> 
}’ $infile > $outfile </br> 

接下来执行preprocess_train.sh，并将结果命名为train_after.csv，命令如下：
> $ chmod +x ./preprocess_train.sh </br> $ ./preprocess_train.sh ./train.csv ./train_after.csv

老师已经给出预处理好的训练集和测试集数据，提供如上方法供参考

## 基于PySpark和Spark MLlib预测回头客

### 启动Hadoop

1. 先启动Hadoop集群和HDFS
> $ cd $HADOOP_HOME </br> $ sbin/start-dfs.sh

2. 再将处理好的训练集和测试集数据上传至HDFS
> $ bin/hadoop dfs -put ~/dbtaobao/dataset/train_after.csv ~/dbtaobao/dataset/test_after.csv /dealu/input

### 使用支持向量机SVM预测回头客
使用Spark MLlib自带的支持向量机SVM分类器进行预测回头客，关于 SVM 的基本介绍：

> 在机器学习中，支持向量机（SVM）是在分类与回归分析中分析数据的监督学习算法。给定一组训练实例，每个训练实例被标记为属于两个类别中的一个或另一个，SVM训练算法建立一个将新的实例分配给两个类别之一的模型，使其成为非概率二元线性分类器。SVM模型是将实例表示为空间中的点，这样映射就使得单独类别的实例被尽可能宽的明显的间隔分开。然后，将新的实例映射到同一空间，并基于它们落在间隔的哪一侧来预测所属类别

下面使用PySpark对其进行实现：
> $ pyspark // 进入 PySpark 交互

1. 导入依赖包
Python
> from pyspark import SparkContext </br> from pyspark.python.pyspark.shell import spark </br> from pyspark.mllib.regression import LabeledPoint </br> from pyspark.mllib.linalg import Vectors,Vector </br> from pyspark.mllib.classification import SVMModel, SVMWithSGD

2. 读取数据
Python
> data = SparkContext.getOrCreate() </br> train_data = data.textFile("hdfs://localhost:8020/dealu/input/train_after.csv") </br> test_data = data.textFile("hdfs://localhost:8020/dealu/input/test_after.csv")

3. 构建模型
通过 map 函数将每行数据用逗号(,)隔开，获取每条数据的字段。我们使用LabeledPoint来存储标签列（label）和特征列（age_range, gender, merchant_id）

> LabeledPoint经常用于存储监督学习中的标签和特征。其要求的数据类型为： </br> * 标签：double </br> * 特征：Vector

Python
> def getData(line): </br> 
&emsp; &emsp; tuples = line.split(',') </br>
&emsp; &emsp; return LabeledPoint(double(tuples[3]), Vectors.dense(double(tuples[0]), double(tuples[1]),double(tuples[2]))) </br> 
train = train_data.map(lambda line: getData(line)) </br>
test = test_data.map(lambda line: getData(line))

4. 使用SGD和训练集构建训练模型并计算预测评分
SGD 即随机梯度下降算法。

> 梯度下降有三种变形形式：</br> * 批量梯度下降法BGD(Batch Gradient Descent): 针对的是整个数据集，通过对所有的样本的计算来求解梯度的方向。全局最优解，当样本数据很多时，计算量开销大，计算速度慢 </br> * 小批量梯度下降法MBGD(Mini-batch Gradient Descent): 把数据分为若干个批，按批来更新参数，这样，一个批中的一组数据共同决定了本次梯度的方向，下降起来就不容易跑偏，减少了随机性 </br> * 随机梯度下降法SGD(Stochastic Gradient Descent): 每个数据都计算算一下损失函数，然后求梯度更新参数。计算速度快，但收敛性能不好

我们可以手动设置算法参数，包括迭代次数，迭代步伐大小），regularization正则化控制参数，每次迭代参与计算的样本比例，weight向量初始值。在这里，我们设置迭代次数为2000
Python
> numIter = 1000 </br> model = SVMWithSGD.train(train, numIter)

然后我们清除阈值，并计算、输出预测评分

Python
> def Getpoint(point): </br> 
&emsp; &emsp; score = model.predict(point.features) </br> 
&emsp; &emsp; return str(score) + " " + str(point.label) </br> 
model.clearThreshold() </br> 
scoreAndLabels = test.map(lambda point: Getpoint(point)) </br> 
scoreAndLabels.foreach(lambda x : print(x)) </br> 

PySpark打印的预测评分部分如下，即为使用训练集和SVM建立的模型为测试集计算的带有确信度的结果

> ...... </br> 
-61296.92625205551 1.0 </br> 
-33795.58530548574 1.0 </br> 
-21516.706402735195 1.0 </br> 
-13204.64745607991 1.0 </br> 
-6434.099796940913 1.0 </br> 
-52589.61866767967 1.0 </br> 
-67751.19180020585 1.0 </br> 
-36183.90673506492 1.0 </br> 
-2386.2332938086206 1.0 </br> 
......
