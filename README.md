# BigData

演示地址: https://vanjker.github.io/BigData/html/

## Build

构建环境:

```bash
$ neofetch --stdout
hadoop@Xubuntu 
-------------- 
OS: Xubuntu 22.04.4 LTS x86_64 
Host: VirtualBox 1.2 
Kernel: 6.5.0-41-generic 
Uptime: 13 mins 
Packages: 1691 (dpkg), 7 (snap) 
Shell: bash 5.1.16 
Resolution: 1920x1080 
DE: Xfce 4.16 
WM: Xfwm4 
WM Theme: Greybird 
Theme: Greybird [GTK2/3] 
Icons: elementary-xfce-darker [GTK2/3] 
Terminal: xfce4-terminal 
Terminal Font: DejaVu Sans Mono 12 
CPU: Intel i7-1065G7 (2) @ 1.497GHz 
GPU: 00:02.0 VMware SVGA II Adapter 
Memory: 2044MiB / 7933MiB 
```

构建步骤:

```bash
# 爬取数据
$ python3.11 bilibili_weekly.py

# 数据预处理并上传至 HDFS
$ python3.11 data_preprocess.py 
$ /usr/local/hadoop/sbin/start-dfs.sh
$ hdfs dfs -put /home/hadoop/bd/bilibili_week.txt /user/hadoop

# 分析数据
$ spark-submit data_analysize1.py 
$ spark-submit data_analysize2.py 

# 依据分析结果生成网页
$ python3.11 echarts_show.py

# 打开演示网页站点
$ open html/index.html
```

## Issues

```
- from notebook.auth from passwd
+ from jupyter_server.auth from passwd
```

## References

- [Bilibili网站“每周必看”栏目数据分析](https://dblab.xmu.edu.cn/blog/4453)
- [Hadoop3.3.5安装教程_单机/伪分布式配置_Hadoop3.3.5/Ubuntu22.04(20.04/18.04/16.04)](https://dblab.xmu.edu.cn/blog/4193)
- [使用Jupyter Notebook调试PySpark程序](https://dblab.xmu.edu.cn/blog/2575)
- [Spark安装和编程实践（Spark3.4.0）](https://dblab.xmu.edu.cn/blog/4322)
