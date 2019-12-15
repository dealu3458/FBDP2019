##基于MapReduce实现的简易K-Means聚类算法

将代码打包后，运行下面命令：

> hadoop jar KMeans.jar <k\> <iteration num\> <input path\> <output path\> 

其中，
k: 簇中心数<br/>
iteration num: 迭代数<br/>
input path: 输入路径<br/>
output path: 输出路径<br/>

#screenshots
screenshots路径中为基于NewInstance.txt中散点的聚类可视化结果，使用R语言中ggplot2包绘制。

图片的命名格式为：K-IterationNum

例如
> 7-10.png代表簇中心数为7，迭代数为10的聚类结果

可以发现，对于同一散点集和簇中心数要求，迭代数越高，K-Means聚类结果越精确。但随着迭代数增高，聚类会趋于某一结果。

因此，在基于大数据集的商业运用中，寻找迭代数（算法运行效率）和聚类结果精确性的平衡至关重要。
