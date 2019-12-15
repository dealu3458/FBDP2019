#关系代数算法 
关系代数算法下一共设置5个子算法:选择Selection，投影Projection，交Intersection，并Join，差Difference，自然连接NaturalJoin。 

这5个子算法需要分别打包，独立运行。
 
这里我们用普通文本文件作为输入文件，展示关系代数算法的流程，我们用作实验的关系有两个RelationA和RelationB。

他们用以记录学生信息。

RelationA关系的格式为：
 id name age weight

1  tom  18  60

3  lily 17  58


RelationB关系文件的格式为:
id genger height

1  1      178

3  0      155

##运行说明
 *Selection

选择以RelationA关系作为输入文件

 输入参数：

 id： 选择的属性号

value： 属性值

act: 行为值。包括0(等于)，1(大于)，2(小于) 

> hadoop jar Selection.jar <input path\> <output path\> <id\> <value\>

例如，要在RelationA中选择年龄大于18的元素只需敲入：

> hadoop jar Selection.jar <input path\> <output path\> 2 18 1  

*Projection
 
投影以RelationA作为关系输入文件

主要输入参数解释： 

col id: 投影的列号

 > hadoop jar Projection.jar <input path\> <output path\> <col id\>

  *Intersection 

交以RelationA作为输入,输入路径下放置两个RelationA关系文件，求交集

 > hadoop jar Intersection.jar <input path\> <output path\> 

*Join 

并以RelationA作为输入,输入路径下放置两个RelationA关系文件，求并集

 > hadoop jar Intersection.jar <input path\> <output path\>  

*Difference 

差以RelationA作为输入文件，这里一关系A的两个输入文件R1.txt和R2.txt来举例 <br/> 

打包后运行:<br> 

> hadoop jar Difference.jar <input path\> <output path\> <relation name\>

其中relation name表示被减的关系文件的名称,例如可以运行: 

> hadoop jar Difference.jar /input/ /output/ R1.txt  

*NaturalJoin 

自然连接以RelationA和RelationB作为输入关系，在列号为0上进行自然连接。

输出的格式为id, name, age, gender, weight, height

 打包后运行：

 > hadoop jar NaturalJoin.jar <input path\> <output path\> 
