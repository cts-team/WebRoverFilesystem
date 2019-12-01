<?php


namespace WebRover\Filesystem\Client\Ftp;


use FtpClient\FtpClient;
use FtpClient\FtpException;
use Symfony\Component\Filesystem\Filesystem;
use WebRover\Filesystem\FilesystemInterface;
use function GuzzleHttp\Psr7\stream_for;

/**
 * Class Client
 * @package WebRover\Filesystem\Client\Ftp
 */
class Client implements FilesystemInterface
{
    /**
     * @var false|string
     */
    private $tmpPath;

    /**
     * @var FtpClient
     */
    private $client;

    /**
     * @var Filesystem
     */
    private $filesystem;

    private $config = [];

    /**
     * Client constructor.
     * @param null $tmpPath
     * @param array $config
     * @throws FtpException
     */
    public function __construct($tmpPath = null, array $config = [])
    {
        $this->tmpPath = $tmpPath ?: getcwd() . DIRECTORY_SEPARATOR . 'WebRover';
        $client = new FtpClient();

        if (!isset($config['ssl'])) $config['ssl'] = false;
        if (!isset($config['port'])) $config['port'] = 21;
        if (!isset($config['timeout'])) $config['timeout'] = 90;
        if (!isset($config['username'])) $config['username'] = 'anonymous';
        if (!isset($config['password'])) $config['password'] = '';

        $this->config = $config;

        $client->connect(
            $config['host'],
            $config['ssl'],
            $config['port'],
            $config['timeout']
        );

        $client->login(
            $config['username'],
            $config['password']
        );

        $this->client = $client;

        $this->filesystem = new Filesystem();
    }

    /**
     * 创建文件夹
     *
     * @param string $path
     * @param null $bucket
     * @return array
     */
    public function mkdir($path, $bucket = null)
    {
        return $this->client->mkdir($path, true);
    }

    /**
     * 移除文件或文件夹
     *
     * @param array|string $paths
     * @param null $bucket
     * @param array $options
     * @throws FtpException
     */
    public function remove($paths, $bucket = null, array $options = [])
    {
        if ($paths instanceof \Traversable) {
            $paths = iterator_to_array($paths, false);
        } elseif (!\is_array($paths)) {
            $paths = [$paths];
        }

        foreach ($paths as $path) {
            if ($this->client->size($path) > -1) {
                $this->client->remove($path);
                continue;
            }

            try {
                $files = $this->client->scanDir($path);
            } catch (\Exception$exception) {
                continue;
            }

            foreach ($files as $key => $file) {
                list($type, $truePath) = explode('#', $key);
                if ($type == 'file') {
                    if (!$this->client->remove($truePath, $options)) {
                        throw new \Exception(sprintf('Failed to remove symlink "%s"', $truePath));
                    }
                } elseif ($type == 'directory') {
                    $this->remove(explode('#', $key, 2)[1]);
                }
            }

            if ($this->client->isEmpty($path)) $this->client->rmdir($path, true);
        }
    }

    /**
     * 移动文件
     *
     * @param $fromPath
     * @param $toPath
     * @param null $fromBucket
     * @param null $toBucket
     * @param array $options
     * @throws FtpException
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
     * @return bool
     */
    public function rename($oldName, $newName, $bucket = null, array $options = null)
    {
        return $this->client->rename($oldName, $newName);
    }

    /**
     * 上传单个文件
     *
     * @param $path
     * @param $content
     * @param null $bucket
     * @param null $options
     * @return bool
     * @throws FtpException
     */
    public function uploadFile($path, $content, $bucket = null, $options = null)
    {
        $info = pathinfo($path);
        if (!is_dir($info['dirname'])) {
            $this->client->mkdir($info['dirname'], true);
        }

        if (file_exists($content)) {
            return $this->client->put($path, $content, FTP_BINARY);
        }

        $this->client->putFromString($path, $content);

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
     * @throws FtpException
     */
    public function uploadPart($path, $content, $partNum, $uploadId = null, $bucket = null, array $options = null)
    {
        $tmpPath = $this->tmpPath . md5($path);

        $partPath = $tmpPath . DIRECTORY_SEPARATOR . "{$partNum}.part";

        return $this->uploadFile($partPath, $content);
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
        $tmpPath = $this->tmpPath . md5($path);

        $parts = $this->client->scanDir($tmpPath);

        $uri = 'ftp://' . $this->config['username'] . ':' . $this->config['password'] . '@' . $this->config['host'] . '/';

        if (!$out = @fopen($uri . $path, 'wb')) {
            throw new \InvalidArgumentException('无法打开存储目录');
        }

        foreach ($parts as $part) {
            $partPath = $uri . $tmpPath . DIRECTORY_SEPARATOR . $part['name'];
            if (!$in = @fopen($partPath, 'rb')) {
                break;
            }
            while ($buff = @fread($in, 4096)) {
                @fwrite($out, $buff);
            }
            @fclose($in);
            $this->client->remove($partPath);
        }

        @fclose($out);

        $this->client->remove($tmpPath, true);

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
     * @throws FtpException
     */
    public function multipartUploadFromFile($path, $file, $bucket = null, array $options = null)
    {
        return $this->uploadFile($path, $file);
    }

    /**
     * 下载文件
     *
     * @param $path
     * @param null $local
     * @param null $bucket
     * @param array|null $options
     * @return bool|string|null
     */
    public function downloadFile($path, $local = null, $bucket = null, array $options = null)
    {
        if (!is_null($local)) {
            return $this->client->getContent($path);
        }

        return $this->client->get($local, $path, FTP_BINARY);
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
     * @throws FtpException
     */
    public function copyFile($fromPath, $toPath, $fromBucket = null, $toBucket = null, array $options = null)
    {
        if ($fromPath != $toPath) {
            $content = $this->client->getContent($fromPath);
            $this->remove($fromPath);
            $this->uploadFile($toPath, $content);
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
        return $this->client->size($path) > -1;
    }

    /**
     * 获取文件元信息
     *
     * @param $path
     * @param null $bucket
     * @param null $options
     * @return \Psr\Http\Message\StreamInterface
     */
    public function getFileMeta($path, $bucket = null, $options = null)
    {
        $content = $this->client->getContent($path);

        return stream_for($content);
    }

    /**
     * 列举文件
     *
     * @param string $prefix
     * @param string $start
     * @param int $size
     * @param null $bucket
     * @return array
     */
    public function listFile($prefix = '', $start = '', $size = 100, $bucket = null)
    {
        return $this->client->scanDir($prefix);
    }
}