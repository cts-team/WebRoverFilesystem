<?php


namespace WebRover\Filesystem\Client\Local;


use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use WebRover\Filesystem\FilesystemInterface;

/**
 * Class Client
 * @package WebRover\Filesystem\Client\Local
 */
class Client implements FilesystemInterface
{
    /**
     * @var Filesystem
     */
    private $filesystem;

    /**
     * @var string
     */
    private $tmpPath;

    /**
     * Client constructor.
     * @param string|null $tmpPath
     */
    public function __construct($tmpPath = null)
    {
        $this->tmpPath = $tmpPath ?: getcwd() . DIRECTORY_SEPARATOR . 'WebRover' . DIRECTORY_SEPARATOR;
        $this->filesystem = new Filesystem();
    }

    /**
     * 创建文件夹
     *
     * @param string $path
     * @param null $bucket
     * @return void
     */
    public function mkdir($path, $bucket = null)
    {
        $this->filesystem->mkdir($path);
    }

    /**
     * 移除文件或文件夹
     *
     * @param array|string $paths
     * @param null $bucket
     * @param array $options
     * @return void
     */
    public function remove($paths, $bucket = null, array $options = [])
    {
        $this->filesystem->remove($paths);
    }

    /**
     * 移动文件
     *
     * @param $fromPath
     * @param $toPath
     * @param null $fromBucket
     * @param null $toBucket
     * @param array $options
     */
    public function move($fromPath, $toPath, $fromBucket = null, $toBucket = null, array $options = [])
    {
        $this->copyFile($fromPath, $toPath);
        $this->remove($fromPath);
    }

    /**
     * 重命名文件或文件夹
     *
     * @param $oldName
     * @param $newName
     * @param null $bucket
     * @param array|null $options
     */
    public function rename($oldName, $newName, $bucket = null, array $options = null)
    {
        $this->filesystem->rename($oldName, $newName, true);
    }

    /**
     * 上传单个文件
     *
     * @param $path
     * @param $content
     * @param null $bucket
     * @param null $options
     * @return bool
     */
    public function uploadFile($path, $content, $bucket = null, $options = null)
    {
        $info = pathinfo($path);
        if (!is_dir($info['dirname'])) {
            $this->filesystem->mkdir($info['dirname']);
        }

        if (file_exists($content)) {
            return $this->copyFile($content, $path);
        }

        $this->filesystem->dumpFile($path, $content);

        return true;
    }

    /**
     * 初始化分片上传
     *
     * @param $path
     * @param null $bucket
     * @param null $options
     * @return string
     */
    public function initiateMultipartUpload($path, $bucket = null, $options = null)
    {
        return md5($path);
    }

    /**
     * 上传单个分片
     *
     * @param $path
     * @param $content
     * @param $partNum
     * @param null $uploadId
     * @param null $bucket
     * @param array|null $options
     * @return bool
     */
    public function uploadPart($path, $content, $partNum, $uploadId = null, $bucket = null, array $options = null)
    {
        $tmpPath = $this->tmpPath . md5($path);

        $partPath = $tmpPath . DIRECTORY_SEPARATOR . "{$partNum}.part";

        $this->filesystem->dumpFile($partPath, $content);

        return true;
    }

    /**
     * 组合分片文件
     *
     * @param $path
     * @param array $uploadParts
     * @param null $uploadId
     * @param null $bucket
     * @return bool
     */
    public function mergeMultipartUpload($path, array $uploadParts = [], $uploadId = null, $bucket = null)
    {
        if (!$out = @fopen($path, 'wb')) {
            throw new \InvalidArgumentException('无法打开存储目录');
        }

        $tmpPath = $this->tmpPath . md5($path);

        $finder = new Finder();

        $finder->depth(0)
            ->name('/\.part$/')
            ->sortByName()
            ->files()
            ->in($tmpPath);

        foreach ($finder as $fileInfo) {
            $partPath = $fileInfo->getPath();
            if (!$in = @fopen($partPath, 'rb')) {
                break;
            }
            while ($buff = @fread($in, 4096)) {
                @fwrite($out, $buff);
            }
            @fclose($in);
            @unlink($partPath);
        }

        @fclose($out);

        $this->filesystem->remove($tmpPath);

        return true;
    }

    /**
     * 分片上传本地文件
     *
     * @param $path
     * @param $file
     * @param null $bucket
     * @param array|null $options
     * @return bool
     */
    public function multipartUploadFromFile($path, $file, $bucket = null, array $options = null)
    {
        return $this->copyFile($file, $path);
    }

    /**
     * 下载文件
     *
     * @param $path
     * @param null $local
     * @param null $bucket
     * @param array|null $options
     * @return bool|false|string
     */
    public function downloadFile($path, $local = null, $bucket = null, array $options = null)
    {
        if (!is_null($local)) {
            return $this->copyFile($path, $local);
        }

        return file_get_contents($path);
    }

    /**
     * 复制文件
     *
     * @param $fromPath
     * @param $toPath
     * @param null $fromBucket
     * @param null $toBucket
     * @param array|null $options
     * @return bool
     */
    public function copyFile($fromPath, $toPath, $fromBucket = null, $toBucket = null, array $options = null)
    {
        if ($fromPath != $toPath) {
            $this->filesystem->copy($fromPath, $toPath, true);
        }

        return true;
    }

    /**
     * 判断文件是否存在
     *
     * @param $path
     * @param null $bucket
     * @param null $options
     * @return bool
     */
    public function fileExist($path, $bucket = null, $options = null)
    {
        return $this->filesystem->exists($path);
    }

    /**
     * 获取文件元信息
     *
     * @param $path
     * @param null $bucket
     * @param null $options
     * @return \SplFileInfo
     */
    public function getFileMeta($path, $bucket = null, $options = null)
    {
        return (new \SplFileInfo($path));
    }

    /**
     * 列举文件
     *
     * @param string $prefix
     * @param string $start
     * @param int $size
     * @param null $bucket
     * @return Finder
     */
    public function listFile($prefix = '', $start = '', $size = 100, $bucket = null)
    {
        $finder = new Finder();

        return $finder->depth(0)->files()->in($prefix);
    }
}
