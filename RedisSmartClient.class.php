<?php

/**
 * @author "王春生" <chunshengster@gmail.com>
 * 关于 twemproxy : https://github.com/twitter/twemproxy
 * ######
 * 注意：如果使用unix socket进行连接，unix socket的路径需要至少包含 “sock”字符
 * ######
 * 1, redis server逐步统一使用 db 0
 * 2, 考虑在每一台PHP Fastcgi的服务器上架设 twemproxy，监听本地socket以及tcp port（分别用于读写连接）
 * 3, twemproxy的local socket用来代理 read only link，本地（127.0.0.1:$port）用来代理master link
 * 4, 考虑 twemproxy的部署以及新上php服务器可能会遗忘部署 twemproxy的情况，通过RedisSmartClient class 来兼容，如果本地没有部署或监听对应的 local socket 或 local ip port，则直接连接已有的redis server
 * 5, RedisSmartClient::connRedis($server_list, $port = 6379, $database = 0) 的第一个参数可以是一个 server list，也可以是一个 ip/socket string，当 $server_list 是一个 list时，程序会逐个连接 list 中的 server，循环直到连接成功或全部失败
 */
class RedisSmartClient {

    private static $ConnSocket = 1;
    private static $ConnTcp = 2;
    private static $ConnTimeout = 3;

    /**
     * public 静态方法，供外部调用
     * @param mixed $server_list
     * @param int $port
     * @param int $database
     * @return boolean|\Redis
     */
    public static function connRedis($server_list, $port = 6379, $database = 0) {

        if (is_array($server_list) and count($server_list) > 0) {
            foreach ($server_list as $server) {
                if (($redisObj = self::_doConnRedis($server, $port, $database,
                                self::$ConnTimeout)) !== FALSE) {
                    return $redisObj;
                }
            }
        } elseif (is_string($server_list) and strlen($server_list) > 0) {
            if (($redisObj = self::_doConnRedis($server_list, $port, $database,
                            self::$ConnTimeout)) !== FALSE) {
                return $redisObj;
            }
        }

        $dataline = date('Y-m-d H:i:s') . __CLASS__ . ':' . __METHOD__ . ':' . 'connect redis error' . 'to host=' . is_array($server_list)
                    ? var_export($server_list, true) : $server_list . ':' . $port;
        GeXingBaseSyslog::doSimpleLog($dataline, "connectreserror");
        return FALSE;
    }

    /**
     * 私有方法，通过判断 $server 的格式进行redis的连接
     * @param string $server
     * @param int $port
     * @param int $database
     * @param int $timeout
     * @return boolean
     */
    private static function _doConnRedis($server, $port = 6379, $database = 0,
            $timeout = 3) {

        if (self::_checkConnStr($server) === self::$ConnSocket) {
           // echo "try $server \n";
            return self::_connRedisViaLocalSocket($server, $database);
        } elseif (self::_checkConnStr($server) === self::$ConnTcp) {
            if (!is_int($port))
                $port = intval($port);
            if (!is_int($database))
                $database = intval($database);
            if (!is_int($timeout))
                $timeout = intval($timeout);
            //echo "try $server $port \n";
            return self::_connRedisViaTcp($server, $port, $database, $timeout);
        }
        return FALSE;
    }

    /**
     * 判断连接目标地址是否 ip 地址，或是 local unix socket
     * 当前判断的标准还比较土鳖
     * @param string $connStr
     * @return self::$ConnSocket|self::$ConnTcp
     */
    private static function _checkConnStr($connStr) {
        $sockStr = 'sock';
        if (strchr($connStr, $sockStr)) {
            return self::$ConnSocket;
        } else {
            return self::$ConnTcp;
        }
    }

    /**
     * 通过 unix socket 连接 redis server
     * @param string $socketStr
     * @param int $database
     * @return False|\Redis
     */
    private static function _connRedisViaLocalSocket($socketStr, $database) {
        $redisObj = new Redis();
        try {
            if ($redisObj->connect($socketStr)) {
                if ($database > 0) {
                    $re = $redisObj->SELECT($database);
                } else {
                    $re = TRUE;
                }
                if (($redisObj instanceof Redis) and isset($redisObj->socket) and ($re
                        === TRUE)) {
                    return $redisObj;
                }
            }
        } catch (RedisException $e) {
            /**
             * @todo 考虑是否通过syslog记录错误日志
             */
            return FALSE;
        }
        return FALSE;
    }

    /**
     * 连接 redis server
     * @param String $server
     * @param int $port
     * @param int $database
     * @param int $timeout
     * @return \Redis|False
     */
    private static function _connRedisViaTcp($server, $port, $database = 0,
            $timeout = 3) {
        $redisObj = new Redis();
        $tmp_count = 0;
        $conn_sec = FALSE;
        while ($tmp_count < 3 and $conn_sec === FALSE) {
            $tmp_count = $tmp_count + 1;
            try {
                if ($redisObj->connect($server, $port, $timeout)) {
                    if ($database > 0) {
                        $conn_sec = $redisObj->SELECT($database);
                    } else {
                        $conn_sec = TRUE;
                    }
                } else {
                    $conn_sec = FALSE;
                }
            } catch (RedisException $e) {
                $conn_sec = FALSE;
            }
        }
        if ($redisObj instanceof Redis and isset($redisObj->socket)) {
            return $redisObj;
        }
        $dataline = date('Y-m-d H:i:s') . __CLASS__ . ':' . __METHOD__ . ':' . 'connect redis error ' . $tmp_count . ' times to host=' .
                $server . ':' . $port;
        //echo $dataline;
//        GeXingBaseSyslog::doSimpleLog($dataline, "connectreserror");
        return FALSE;
    }

    /**
     * for testing
     *
      require"./RedisSmartClient.php";

      $hostA = array(
      'host' => '172.16.3.6',
      'port' => 6387,
      'database' => 0
      );
      $hostB = array(
      'host' => '/var/run/nutcracker_redis_6387.sock',
      'port' => 6387,
      'database' => 0
      );
      $hostC = array(
      'host' => array('/var/run/nutcracker_redis_6387.sock', '172.16.3.6', '172.16.3.8'),
      'port' => 6387,
      'database' => 0
      );
      $hostD = array(
      'host' => array('/var/run/nutcracker_redis_6387fds.sock', '172.16.3.6', '172.16.3.8'),
      'port' => 6387,
      'database' => 0
      );
      echo "=============================================================\n";
      var_dump($hostA);
      $redisObjA = RedisSmartClient::connRedis($hostA['host'],
      $hostA['port']);
      var_dump($redisObjA);

      echo "=============================================================\n";
      $redisObjB = RedisSmartClient::connRedis($hostB['host'],
      $hostB['port']);
      var_dump($redisObjB);
      echo "=============================================================\n";
      $redisObjC = RedisSmartClient::connRedis($hostC['host'],
      $hostC['port']);
      var_dump($redisObjC);
      echo "=============================================================\n";
      $redisObjD = RedisSmartClient::connRedis($hostD['host'],
      $hostD['port']);
      var_dump($redisObjD);

     */
}

?>
