(:
 : Copyright 2010 XQuery.co.uk
 :
 : Licensed under the Apache License, Version 2.0 (the "License");
 : you may not use this file except in compliance with the License.
 : You may obtain a copy of the License at
 :
 : http://www.apache.org/licenses/LICENSE-2.0
 :
 : Unless required by applicable law or agreed to in writing, software
 : distributed under the License is distributed on an "AS IS" BASIS,
 : WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 : See the License for the specific language governing permissions and
 : limitations under the License.
:)

(:~
 : <p>
 : </p>
 :
 : @author Klaus Wichmann klaus [at] xquery [dot] co [dot] uk
 :)
module namespace request = 'http://www.xquery.co.uk/modules/connectors/aws/s3/request';

import module namespace util = 'http://www.xquery.co.uk/modules/connectors/aws/helpers/utils' at '../helpers/utils.xq';
import module namespace common_request = 'http://www.xquery.co.uk/modules/connectors/aws/helpers/request' at '../helpers/request.xq';
import module namespace factory = 'http://www.xquery.co.uk/modules/connectors/aws/s3/factory' at '../s3/factory.xq';

import module namespace http = "http://expath.org/ns/http-client";
import module namespace ser = "http://www.zorba-xquery.com/modules/serialize";
import module namespace base64 = "http://www.zorba-xquery.com/modules/base64";
import module namespace error = 'http://www.xquery.co.uk/modules/connectors/aws/s3/error' at '../s3/error.xq';

(:~
 : send an http request and return the response which is usually a pair of two items: One item containing the response
 : headers, status code,... and one item representing the response body (if any).
 :
 : @return the http response
:)
declare sequential function request:send($request as element(http:request)) as item()* {

    let $response := http:send-request($request)
    let $status := number($response[1]/@status)
    return
        
        if($status = (200,204)) 
        then $response
        else error:throw($status,$response);
};


(:~
 : add an acl grant to the create request
 : 
:)
declare updating function request:add-acl-everybody(
    $request as element(http:request),$acl as xs:string?){

    let $acl-header := <http:header name="x-amz-acl" value="{$acl}" />
    return
        (
            if($acl) 
            then insert node $acl-header as first into $request 
            else ()
        ) 
};

(:~
 : Add a header to indicate that the value of an object should be copied from a source object instead of
 : from the body of a request.
 : 
:)
declare updating function request:add-copy-source(
    $request as element(http:request),$source-bucket as xs:string, $source-object as xs:string){

    let $copy-source-header := <http:header name="x-amz-copy-source" value="{concat($source-bucket, "/", $source-object)}" />
    return
        insert node $copy-source-header as first into $request 
};

(:~
 : Add a header to indicate that the metadata of the source object of a copy operation should be overwritten 
 : instead of being copied.
 : 
:)
declare updating function request:add-replace-metadata-flag($request as element(http:request)){

    insert node 
        <http:header name="x-amz-metadata-directive" value="REPLACE" />
        as first into $request 
};

(:~
 : add an acl policy to the request
 : 
:)
declare updating function request:add-acl-grantee(
    $request as element(http:request),$acl as element(AccessControlPolicy)){

    common_request:add-content-xml($request,$acl)
};


(:~
 : add a bucket logging status to the request
 : 
:)
declare updating function request:add-bucket-logging(
    $request as element(http:request),
    $logging-config as element(BucketLoggingStatus)){

    common_request:add-content-xml($request,$logging-config)
};

(:~
 : add a bucket notification status to the request
 : 
:)
declare updating function request:add-bucket-notification(
    $request as element(http:request),
    $notification-config as element(NotificationConfiguration)){

    common_request:add-content-xml($request,$notification-config)
};

(:~
 : add a specific location configuration to the request
 : 
:)
declare updating function request:add-create-bucket-location($request as element(http:request),$location as xs:string?){

    if($location)
    then 
        let $config := factory:config-create-bucket-location($location) 
        let $content := ser:serialize($config)
        return common_request:add-content-text($request,$content)
    else ()
};

(:~
 : add an metadata to the request
 : 
:)
declare updating function request:add-metadata($request as element(http:request),$metadata as element()*){

    for $meta in $metadata
    let $name := concat("x-amz-meta-",$meta/local-name())
    let $value := string($meta/text())
    let $meta-header := <http:header name="{$name}" value="{$value}" />
    return
        insert node $meta-header as first into $request 
};

(:~
 : Add reduced-redundancy flag to the request. This function simply turnes the reduced redundancy on by
 : passing the header x-amz-storage-class=REDUCED_REDUNDANCY.
 : 
:)
declare updating function request:add-reduced-redundancy($request as element(http:request)){

    let $name := "x-amz-storage-class"
    let $value := "REDUCED_REDUNDANCY"
    let $meta-header := <http:header name="{$name}" value="{$value}" />
    return
        insert node $meta-header as first into $request 
};

(:~
 : add a bucket request-payment-config to the request
 : 
:)
declare updating function request:add-bucket-request-payment-config(
    $request as element(http:request),
    $request-payment-config as element(RequestPaymentConfiguration)){

    common_request:add-content-xml($request,$request-payment-config)
};

(:~
 : add a bucket versioning-config to the request
 : 
:)
declare updating function request:add-bucket-versioning-config(
    $request as element(http:request),
    $versioning-config as element(VersioningConfiguration)){

    common_request:add-content-xml($request,$versioning-config)
};
