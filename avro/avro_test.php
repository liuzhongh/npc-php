<?php
/**
 * Created by PhpStorm.
 * User: liuzh
 * Date: 14-10-24
 * Time: 上午10:29
 */
require_once('lib/avro.php');

$paramProtocol = file_get_contents('/media/E/work/javaProjects/npc/src/test/code/core/npc/test/Service1.avpr');
$schema = AvroProtocol::parse($paramProtocol);

$client = new Client('127.0.0.1', 8080);

$args = array('str' => 'it is php');

$result = ClientRequestor::getClient($client, 'helloWorld', 'getTest', $schema, $args);

echo($result);
echo('</br>');

$result1 = ClientRequestor::getClient($client, 'helloWorld2', 'getTest', $schema, $args);

echo($result1);
echo('</br>');

$result1 = ClientRequestor::getClient($client, 'helloWorld2', 'getNoArg', $schema);

echo($result1);
echo('</br>');

$client->close();
