package org.apache.hadoop.fs.lustre;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class LustreFileSystem extends FileSystem {
	
	private URI uri = URI.create("lfs:///");
	private Path workingDir;
	
	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf);
		setConf(conf);
		workingDir = getHomeDirectory();
	}

	@Override
	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress) throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public FSDataOutputStream create(Path path, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		if (exists(path) && !overwrite) {
			throw new IOException("File already exists: " + path);
		}
		Path parent = path.getParent();
		if (parent != null && !mkdirs(parent)) {
			throw new IOException("Mkdirs failed to create " + parent.toString());
		}
		
		return new FSDataOutputStream(new BufferedOutputStream(
				new LFSOutputStream(path, false), bufferSize), statistics);
	}

	@Override
	public boolean delete(Path path) throws IOException {
		return delete(path, true);
	}

	@Override
	public boolean delete(Path path, boolean recursive) throws IOException {
		File file = pathToFile(path);
		if (file.isFile()) {
			return file.delete();
		} else if ((!recursive) && file.isDirectory() &&
				(file.listFiles().length != 0)){
			throw new IOException("Directory " + file.toString() + " is not empty");
		}
		
		return FileUtil.fullyDelete(file);
	}
	
	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus stat, 
			long start, long len) {
		// not implemented yet
		
		if (stat == null) {
			return null;
		}
		
		if ((start < 0) || (len < 0)) {
			throw new IllegalArgumentException("Invalid start or len parameter");
		}
		
		if (stat.getLen() < start) {
			return new BlockLocation[0];
		}
		
		String[] name = { "localhost:50010" };
		String[] host = { "localhost" };
		
		return new BlockLocation[] { new BlockLocation(name, host, 0, stat.getLen())};
	}

	@Override
	public FileStatus getFileStatus(Path path) throws IOException {
		File file = pathToFile(path);
		if (file.exists()) {
			return new FileStatus(file.length(), file.isDirectory(), 1, getDefaultBlockSize(), file.lastModified(),
					new Path(file.getPath()).makeQualified(this));
		} else {
			throw new FileNotFoundException("File " + path + " does not exist.");
		}
	}

	@Override
	public Path getHomeDirectory() {
		return new Path(FileSystem.getDefaultUri(getConf()).getPath(),
				System.getProperty("user.name")).makeQualified(this);
	}

	@Override
	public URI getUri() {
		return uri;
	}

	@Override
	public Path getWorkingDirectory() {
		return workingDir;
	}

	@Override
	public FileStatus[] listStatus(Path f) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean mkdirs(Path path) {
		Path parent = path.getParent();
		File file = pathToFile(path);
		return (parent == null || mkdirs(parent)) &&
			(file.mkdir() || file.isDirectory());
	}
	
	@Override
	public boolean mkdirs(Path path, FsPermission permission) throws IOException {
		boolean res = mkdirs(path);
		setPermission(path, permission);
		return res;
	}

	@Override
	public FSDataInputStream open(Path path, int bufferSize) throws IOException {
		if (!exists(path)) {
			throw new FileNotFoundException(path.toString());
		}

		return new FSDataInputStream(new BufferedFSInputStream(
				new LFSInputStream(path), bufferSize));
	}
	
	public File pathToFile(Path path) {
		checkPath(path);
		if (!path.isAbsolute()) {
			path = new Path(this.getWorkingDirectory(), path);
		} 
		return new File(path.toUri().getPath());
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		if (pathToFile(src).renameTo(pathToFile(dst))) {
			return true;
		}
		return FileUtil.copy(this, src, this, dst, true, getConf());
	}

	@Override
	public void setWorkingDirectory(Path newDir) {
		workingDir = newDir;
	}

	class LFSInputStream extends FSInputStream {
		private FileInputStream fis;
		private long position;
		
		public LFSInputStream(Path path) throws IOException {
			fis = new FileInputStream(pathToFile(path));
		}

		@Override
		public long getPos() throws IOException {
			return this.position;
		}

		@Override
		public void seek(long pos) throws IOException {
			fis.getChannel().position(pos);
			this.position = pos;
		}

		@Override
		public boolean seekToNewSource(long targetPos) throws IOException {
			return false;
		}

		@Override
		public int read() throws IOException {
			try {
				int res = fis.read();
				if (res > 0) {
					this.position++;
				}
				return res;
			} catch (IOException e) {
				throw new FSError(e);
			}
		}
	}

	class LFSOutputStream extends OutputStream implements Syncable {
		private FileOutputStream fos;
		
		public LFSOutputStream(Path path, boolean append) throws IOException {
			this.fos = new FileOutputStream(pathToFile(path), append);
		}

		@Override
		public void write(int b) throws IOException {
			try {
				fos.write(b);
			} catch (IOException e) {
				throw new FSError(e);
			}
		}

		@Override
		public void sync() throws IOException {
			fos.getFD().sync();
		}
		
	}
}
