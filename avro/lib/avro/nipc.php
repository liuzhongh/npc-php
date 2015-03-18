<?php
/**
 * Created by PhpStorm.
 * User: liuzh
 * Date: 14-10-27
 * Time: 下午4:03
 */
define('PROTOCOL_REQUEST', file_get_contents(str_replace('\\','/',dirname(__FILE__)).'/NHandshakeRequest.avsc'));
define('PROTOCOL_RESPONSE', file_get_contents(str_replace('\\','/',dirname(__FILE__)).'/NHandshakeResponse.avsc'));

class Client
{
    private $socket;

    function __construct($host, $port, $timeout=5000)
    {
        $this->socket = stream_socket_client($host . ':' . $port, $errno, $errstr, $timeout);
        if (!$this->socket)
            throw new Exception($errstr, $errno);
    }

    public function getConnection()
    {
        return $this->socket;
    }

    public function close()
    {
        fclose($this->socket);
    }

}

class ClientRequestor
{
    public static function getClient($client, $instanceName, $methodName, $protocol, $args=array(), $id=1, $timeout=50000, $protocolType=1)
    {
        $requestor = new ClientRequestor();
        return $requestor->get_Client($client, $instanceName, $methodName, $protocol, $args, $id, $timeout, $protocolType);
    }

    function get_Client($client, $instanceName, $methodName, $protocol, $args, $id=1, $timeout=50000, $protocolType=1)
    {
        if (is_null($instanceName))
            throw new AvroException("Instance name can't be null!");
        if (is_null($methodName))
            throw new AvroException("Method name can't be null!");
        if (is_null($protocol))
            throw new AvroException("Protocol can't be null!");


        $socket = $client->getConnection();
        $message = $protocol->messages[$methodName];

        $this->request($socket, $instanceName, $methodName, $message, $args, $id, $timeout, $protocolType);

        return $this->response($socket, $message);
    }

    private function request($socket, $instanceName, $methodName, $message, $args, $id=1, $timeout=50000, $protocolType=1)
    {
        $datum_writer = new AvroIODatumWriter(AvroSchema::parse(PROTOCOL_REQUEST));
        $write_io = new AvroStringIO();
        $encoder = new AvroIOBinaryEncoder($write_io);

        $handshakeRequest = array('id' => $id,
            'timeout' => $timeout,
            'protocolType' => $protocolType,
            'targetInstanceName' => $instanceName,
            'methodName' => $methodName);
        $datum_writer->write($handshakeRequest, $encoder);

        $datum_writer = new AvroIODatumWriter($message->request);
        $datum_writer->write($args, $encoder);

        $content = $write_io->string();
        $length = $write_io->length();

        $write_io->close();

        $header = (pack('N2', '1', '1'));

        fwrite($socket, $header);
        fwrite($socket, pack('N', $length));
        fwrite($socket, $content);
    }

    private function response($socket, $message)
    {
        $resultHeader = fread($socket, 8);
        $c = unpack('N2', $resultHeader);
        $datum_reader = new AvroIODatumReader(AvroSchema::parse(PROTOCOL_RESPONSE));

        $read_io = new AvroStringIO();

        for ($i = 0; $i < $c[2]; $i++) {
            $buff = new AvroStringIO();
            $size = unpack('N', fread($socket, 4));
            $size = $size[1];
            while ($buff->tell() < $size) {
                $chunk = fread($socket, $size - $buff->tell());
                if ($chunk == '')
                    throw new AvroException("socket read 0 bytes");
                $buff->write($chunk);
            }
            $read_io->write($buff->string());
            $buff->close();
        }

        $decoder = new AvroIOBinaryDecoder(new AvroStringIO($read_io->string()));

        $read_io->close();

        $handshakeResponse = $datum_reader->read($decoder);
        $isError = $decoder->read_boolean();

        if(!$isError){
            $schema_res = $message->response;
            $datum_reader = new AvroIODatumReader($schema_res);
            return $datum_reader->read($decoder);
        }else{
            /**
             * TODO 缺少获取errors schema
            $schema_res = $message;
            $datum_reader = new AvroIODatumReader($schema_res);
            */
            throw new AvroException('返回结果出错！');
        }

    }
}