# Coding Release 发布工具


## 简介

用于创建 Coding 发布使用的 Release Checklist 文档

## 安装

windows 用户请先设置环境变量 $GOBIN

```bash
curl https://raw.githubusercontent.com/suuuy/influx-import/master/install.sh | sh
```

## 导入命令

```
influx-import --database="root:coding123@tcp(127.0.0.1:3306)/coding_statistic" --influx-url="http://127.0.0.1:18086/write?db=statistic&u=root&p=coding123" --chunk=1000
```
