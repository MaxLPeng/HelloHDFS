package pl.com.hdfs;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HelloHDFS {

	final static String FS_DEFAULTFS 		= "hdfs://NameNode01:9000"; 
	//final static String FS_DEFAULTFS 		= "hdfs://namecluster1"; 
	final static String DFS_PATH_ROOT 		= "/test/in";
	final static String DFS_FILE 			= "/HDFSHello.data";
	final static String Local_PATH 			= "e:/tmp";
	final static String Local_FILE 			= "/HDFSHello.txt";

	static FileSystem _fileSystem = null;
	
	public static void main(String[] args) throws IOException {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", FS_DEFAULTFS);
		conf.set("dfs.replication", "2");
		_fileSystem = FileSystem.get(conf); 
		
		uploadDFS();
		
		String dfsFile = FS_DEFAULTFS + DFS_PATH_ROOT + DFS_FILE; 
		System.out.println(dfsFile + "内容：");
		getDFSfile(dfsFile);
	}

	/**
	 * DFS 上传文件 
	 * @throws IOException
	 */
	public static void uploadDFS() throws IOException { 
		boolean reuslt = false ; 
				
		// 判断目录
		reuslt = _fileSystem.exists(new Path(DFS_PATH_ROOT));
		System.out.println(DFS_PATH_ROOT + " exists=" + reuslt);

		// 如果存在，先删除
		if (reuslt) {
			reuslt = _fileSystem.delete(new Path(DFS_PATH_ROOT), true);
			System.out.println(DFS_PATH_ROOT + " delete=" + reuslt);
		}
		
		// 创建目录
		reuslt = _fileSystem.mkdirs(new Path(DFS_PATH_ROOT));
		System.out.println(DFS_PATH_ROOT + " mkdirs=" + reuslt);

		// 上传文件
		String fileDFS = DFS_PATH_ROOT + DFS_FILE;
		String fileLocal = Local_PATH + Local_FILE;
		FSDataOutputStream out = _fileSystem.create(new Path(fileDFS), true);
		FileInputStream fis = new FileInputStream(fileLocal);
		IOUtils.copyBytes(fis, out, 2048, true);
		System.out.println("文件" + fileLocal + " 传输至 " + fileDFS + "...OK!");

		// 显示目录信息
		System.out.println(DFS_PATH_ROOT + "目录：");
		getDFSPathFull(_fileSystem,new Path("/"));

	} 
	 
	
	/**
	 * HDFS 显示目录信息 
	 * @throws IOException
	 */
	public static void getDFSPath(FileSystem pfileSystem) throws IOException {
		FileStatus[] statuses = pfileSystem.listStatus(new Path("/"));
		for (FileStatus status : statuses) {
			System.out.println(status.getPermission() 
					+ " " + status.getReplication() 
					+ " " + status.getPath().toString().replace(FS_DEFAULTFS, ""));
		}
	}

	/**
	 * 
	 * @param hdfs FileSystem 对象
	 * @param path 文件路径
	 */
	public static void getDFSPathFull(FileSystem hdfs, Path path) {
		try {
			if (hdfs == null || path == null) {
				return;
			}
			// 获取文件列表
			FileStatus[] files = hdfs.listStatus(path);

			// 展示文件信息
			for (int i = 0; i < files.length; i++) {
				try {
					if (files[i].isDirectory()) {
						System.out.println(
								files[i].getPermission() 
								+ "  " + files[i].getOwner() 
								+ "  " + files[i].getReplication()
								+ " ..." + "  " + files[i].getPath().toString().replace(FS_DEFAULTFS, "")

						);
						// 递归调用
						getDFSPathFull(hdfs, files[i].getPath());
					} else if (files[i].isFile()) {
						System.out.println(files[i].getPermission() 
								+ "  " + files[i].getOwner() 
								+ "  " + files[i].getReplication() 
								+ "   " + "   " + files[i].getPath().toString().replace(FS_DEFAULTFS, "") 
								+ "   " + files[i].getLen());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * DFS文件 获取 
	 * @throws IOException
	 */
	public static void getDFSfile(String DFSfile) throws IOException {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		InputStream in = new URL(DFSfile).openStream();
		IOUtils.copyBytes(in, System.out, 4096, true);
	}
}
