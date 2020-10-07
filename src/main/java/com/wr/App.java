package com.wr;


import com.sun.imageio.plugins.common.I18N;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

/**
 * Hello world!
 */
public class App {
    //创建配置对象
    private static final Configuration CONF = new Configuration();
    private static final String USER = "root";

    public static void main(String[] args) {

        FileSystem fs = null;
        try {
            //获取hdfs的客户端
            fs = FileSystem.get(new URI("hdfs://centos201:9000"), CONF, USER);
            Path srcPath = new Path(args[0]);
            Path distPath = new Path(args[1]);
            //1.文件上传
            //Path srcPath = new Path("C:\\Users\\26251\\Desktop\\a.txt");
            //Path distPath = new Path("/usr/a.txt");
            //upload(fs, srcPath, distPath);

            //2.文件下载
            //  /usr/a.txt  C:\Users\26251\Desktop\a.txt

            //getFile(fs, srcPath, distPath);

            //3.文件读取打印
            //printConsole(fs, srcPath);

            //4.文件信息
            //getFileInfo(fs, srcPath);

            //5.所有文件信息
            //getAllFileInfo(fs, srcPath);

            //6.文件创建和删除
            //createOrDeleteFile(fs,srcPath);

            //7.目录创建和删除
            //createOrDeleteDir(fs,srcPath);

            //8.内容追加
            //appendFile(fs, srcPath);

            //9.删除文件
            //deleteFile( fs,srcPath);

            //10.移动
            moveTodist(fs, srcPath, distPath);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private static void upload(FileSystem fs, Path srcPath, Path distPath) throws Exception {
        if (!fs.exists(distPath)) {
            fs.copyFromLocalFile(false, srcPath, distPath);
            System.out.println("文件上传成功！");
        } else {
            System.out.println("---文件已存在！---");
            System.out.println("---1表示追加到原有文件末尾---");
            System.out.println("---2表示覆盖原有文件---");

            boolean flag = true;
            while (flag) {
                System.out.print("请输入你的选择：");
                Scanner sc = new Scanner(System.in);
                int n = sc.nextInt();
                switch (n) {
                    case 1:
                        //支持追加操作
                        CONF.setBoolean("dfs.support.append", true);
                        //有节点出错时，也执行追加操作
                        CONF.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
                        CONF.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");

                        FSDataOutputStream outputStream = fs.append(distPath);
                        InputStream inputStream = new FileInputStream(srcPath.toString());
                        IOUtils.copyBytes(inputStream, outputStream, 2048, true);
                        System.out.println("文件追加成功！");
                        flag = false;
                        break;
                    case 2:
                        fs.copyFromLocalFile(false, srcPath, distPath);
                        System.out.println("文件覆盖成功！");
                        flag = false;
                        break;
                    default:
                        flag = true;
                        break;
                }
            }
        }
        //4 关闭资源
        fs.close();
    }

    public static void getFile(FileSystem fs, Path srcPath, Path distPath) {
        try {
            File file = new File(distPath.toString());
            if (file.exists()) {
                String[] split = distPath.toString().split("\\.");
                //有同名文件则重命名
                String path = split[0] + System.currentTimeMillis() + "." + split[1];
                System.out.println("下载路径：" + path);
                distPath = new Path(path);
            }
            fs.copyToLocalFile(srcPath, distPath);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void printConsole(FileSystem fs, Path srcPath) {
        try {
            FSDataInputStream inputStream = fs.open(srcPath);
            IOUtils.copyBytes(inputStream, System.out, 2048, true);
            fs.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void getFileInfo(FileSystem fs, Path srcPath) {
        try {
            FileStatus[] status = fs.listStatus(srcPath);
            for (FileStatus fileStatus : status) {
                long time = fileStatus.getModificationTime();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                System.out.println("读写权限：" + fileStatus.getPermission());
                System.out.println("文件大小：" + fileStatus.getLen());
                System.out.println("创建时间：" + dateFormat.format(new Date(time)));
                System.out.println("文件路径：" + fileStatus.getPath().toString());
                System.out.println("---------------------");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void getAllFileInfo(FileSystem fs, Path srcPath) {
        try {
            FileStatus[] listStatus = fs.listStatus(srcPath);
            for (FileStatus status : listStatus) {
                Path path = status.getPath();
                if (fs.isDirectory(path)) {
                    System.out.println("走了这个方法");
                    getAllFileInfo(fs, path);
                } else {
                    getFileInfo(fs, path);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createOrDeleteFile(FileSystem fs, Path srcPath) {
        try {
            System.out.println("1表示创建文件");
            System.out.println("2表示删除文件");
            System.out.println("3表示退出");
            Scanner sc = new Scanner(System.in);

            boolean flag = true;
            while (flag) {
                System.out.print("请输入：");
                int i = sc.nextInt();
                switch (i) {
                    case 1:
                        if (!fs.exists(srcPath)) {
                            fs.createNewFile(srcPath);
                            System.out.println("文件创建成功！");
                        } else {
                            System.out.println("文件已存在，无需创建！");
                        }
                        flag = true;
                        break;
                    case 2:
                        if (fs.exists(srcPath)) {
                            //参数1:要删除的路径
                            //参数2：是否递归删除，如果要删除目录，该参数需要指定为true
                            boolean delete = fs.delete(srcPath, false);
                            if (delete) {
                                System.out.println("文件删除成功");
                            } else {
                                System.out.println("文件删除失败");
                            }
                        } else {
                            System.out.println("删除的文件不存在！");
                        }
                        flag = true;
                        break;

                    default:
                        flag = false;
                        break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createOrDeleteDir(FileSystem fs, Path srcPath) {
        try {
            System.out.println("1表示创建目录");
            System.out.println("2表示删除目录");
            System.out.println("3表示退出");
            Scanner sc = new Scanner(System.in);

            boolean flag = true;
            while (flag) {
                System.out.print("请输入：");
                int n = sc.nextInt();
                switch (n) {
                    case 1:
                        if (!fs.exists(srcPath)) {
                            fs.mkdirs(srcPath);
                            System.out.println("目录创建成功！");
                        } else {
                            System.out.println("目录已存在，无需创建！");
                        }
                        flag = true;
                        break;
                    case 2:
                        if (fs.exists(srcPath)) {
                            FileStatus[] status = fs.listStatus(srcPath);
                            int i = 0;
                            for (FileStatus fileStatus : status) {
                                Path path = fileStatus.getPath();
                                System.out.println(path.toString());
                                if (fileStatus.isFile()) {
                                    i = 1;
                                }
                            }
                            System.out.println(i);
                            if (i == 1) {
                                System.out.println("目录中存在文件，确定删除？Y or N");
                                System.out.println("请输入你的选择：");
                                Scanner scanner = new Scanner(System.in);
                                String next = scanner.next();
                                if ("Y".equals(next)) {
                                    //迭代删除目录下的文件
                                    for (FileStatus fileStatus : status) {
                                        Path path = fileStatus.getPath();
                                        fs.delete(path, true);
                                    }
                                    boolean delete = fs.delete(srcPath, false);
                                    if (delete) {
                                        System.out.println("目录删除成功");
                                    } else {
                                        System.out.println("目录删除失败");
                                    }
                                } else {
                                    System.out.println("目录没有删除！");
                                }
                            } else {
                                System.out.println("目录为空！");
                                boolean delete = fs.delete(srcPath, false);
                                if (delete) {
                                    System.out.println("目录删除成功");
                                } else {
                                    System.out.println("目录删除失败");
                                }

                            }
                        } else {
                            System.out.println("路径不存在！");
                        }

                        break;

                    default:
                        flag = false;
                        break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void appendFile(FileSystem fs, Path srcPath) {
        System.out.println("1表示追加到末尾");
        System.out.println("2表示追加到开头");
        System.out.println("3表示展示内容");
        System.out.println("4表示退出");

        boolean flag = true;
        try {
            while (flag) {


                Scanner sc = new Scanner(System.in);
                System.out.print("请选择你的操作：");
                int n = sc.nextInt();
                switch (n) {
                    case 1:
                        System.out.print("请输入追加的内容：");
                        String next = sc.next();

                        //多次建立连接，每次追加后就关闭
                        FileSystem fs1 = FileSystem.get(new URI("hdfs://centos201:9000"), CONF, USER);
                        FSDataOutputStream outputStream = fs1.append(srcPath);
                        //默认写到文件末尾
                        outputStream.write(next.getBytes());
                        outputStream.flush();
                        fs1.close();
                        break;
                    case 2:
                        System.out.print("请输入追加的内容：");
                        next = sc.next();

                        String localPath = "d:\\aa.txt";
                        File file = new File(localPath);
                        if (!file.exists()) {
                            file.createNewFile();
                        }
                        FileSystem fs2 = FileSystem.get(new URI("hdfs://centos201:9000"), CONF, USER);
                        //1.拷贝到本地，删除原文件
                        fs2.copyToLocalFile(true, srcPath, new Path(localPath));
                        //2.创建空的原文件
                        fs.createNewFile(srcPath);
                        FSDataOutputStream stream = fs.append(srcPath);
                        //3.追加输入的内容到文件开头
                        stream.write(next.getBytes());
                        //4.原有内容拷贝
                        FileInputStream inputStream = new FileInputStream(new File(localPath));
                        IOUtils.copyBytes(inputStream, stream, 2048, true);
                        fs2.close();

                        break;
                    case 3:
                        FileSystem fileSystem = FileSystem.get(new URI("hdfs://centos201:9000"), CONF, USER);
                        FSDataInputStream open = fileSystem.open(srcPath);
                        IOUtils.copyBytes(open, System.out, 2048, true);
                        System.out.println();
                        fileSystem.close();
                        break;
                    default:
                        flag = false;
                        break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private static void deleteFile(FileSystem fs, Path srcPath) {
        try {
            if (fs.exists(srcPath)) {
                //第二个参数表示 不递归删除
                fs.delete(srcPath, false);
            } else {
                System.out.println("删除的文件不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void moveTodist(FileSystem fs, Path srcPath, Path disPath) {

        try {
            //FSDataInputStream open = fs.open(srcPath);
            //if (fs.exists(disPath)){
            //    fs.delete(disPath,false);
            //}
            //fs.createNewFile(disPath);
            //FSDataOutputStream outputStream = fs.append(disPath);
            //IOUtils.copyBytes(open,outputStream,2048,true);
            //fs.delete(srcPath,false);
            //fs.close();
            fs.rename(srcPath, disPath);
            System.out.println("移动成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
