#  作业4 MatrixMultiply 说明

##使用方法
将源码打包成jar包后，在系统节点上执行如下命令：
>$ bin/hadoop jar MatrixMultiply.jar <Matrix M input path> <Matrix N input path> <output path> <M row #> <M column #> <N column #>

##矩阵生成
需保证两个矩阵输入文件的命名格式为
>M_X_Y, N_A_B
其中X, Y, A, B分别为矩阵M的行和列数和N的行和列数。
且矩阵输入路径中只有两个输入文件或两个文件在同一路径, 否则会影响三个关键参数的解析

##关键解释
代码中解析三个关键参数使用了com.java.io.File中File.list( ) API自动解析输入路径中文件名称
但在HDFS中由于不明原因使用File.list解析始终出错，因而用用户手动输入三个关键参数代替
