# Hadoop---Matrix-Multiplication

我們將整個架構拆成兩個job，第一個job計算mij x njk，第二個job計算summation。

## MultiplicationMapper
依次讀取一行資料，並用”,” 分割出我們需要的資料。  
以j 值為key，將剩餘資料放入IntTupleWritable中做為key  
> Input: line  
>
> M,0,0,10  
> M,0,1,0  
> N,0,0,1  
> N,1,0,2 
>
> output: key <j>  value <Matrix, row index, element>          
>  < 0 >    <M, 0, 10>  
>  < 1 >    <M, 0, 0>  
>  < 0 >    <N, 0, 1>  
>  < 1 >    <N, 0, 2>  
  
## MultiplicationReducer
遍歷Iterable<IntTupleWritable> values 並將其分為M、N兩個矩陣。 	
對矩陣的每個element相乘(m_ij×n_jk)。
>input: key <j>  value <Matrix, row index, element>
>  < 0 >    [ <M, 0, 10> , <N, 0, 1>]  
>  < 1 >    [ <M, 0, 0> , <N, 0, 2> ]  
>  
>Output: key <i, k>  value <v>
>  < 0, 0 >  <10×1>  
>  < 0, 0 >  <0×2> 	

## SumMapper
利用空白分割出key與value，並直接傳至reducer。
>Intput: line
>  (0, 0)  10  
>  (0, 0)  0>  
>  
>Output: key <i, k>  value <v>  
>  < 0, 0 >  <10>  
>  < 0, 0 >  <0> 	

## SumReducer
將同樣(I,k)的值加總。
>Input: key <i, k>  value<v>  
>  <0, 0>  [ <10> , <0> ]  
>Output: <I, k, summation value>  
>0,0,10   
