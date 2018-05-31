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
module namespace aws-request = 'http://www.xquery.co.uk/modules/connectors/aws/helpers/request';

import module namespace aws-utils = 'http://www.xquery.co.uk/modules/connectors/aws/helpers/utils' at 'utils.xq';

import module namespace crypto = "http://expath.org/ns/crypto";
(:
import module namespace console="http://exist-db.org/xquery/console";
:)
import module namespace http="http://expath.org/ns/http-client";

(:
import module namespace base64 = "http://www.zorba-xquery.com/modules/base64";
import module namespace hash = "http://www.zorba-xquery.com/modules/security/hash";
import module namespace hmac = "http://www.zorba-xquery.com/modules/security/hmac";
import module namespace ser = "http://www.zorba-xquery.com/modules/serialize";
:)


(:~
 : create an http request
 :
 : @return the newly created http request
:)
declare function aws-request:create($method as xs:string,$href as xs:string) as element(http:request) {

    <http:request method="{$method}"
                  href="{$href}"
                  http-version="1.1"
                  chunked="false">
        <http:header name="x-amz-date" value="{aws-utils:x-amz-date()}" />
        <http:header name="Date" value="{aws-utils:http-date()}" />
    </http:request>
};

(:~
 : create an http request with a query string build with the provided parameters
 :
 : @return the newly created http request
:)
declare function aws-request:create($method as xs:string,$href as xs:string,$parameters as element(parameter)*) as element(http:request) {

    let $query := 
        string-join(
            for $param in $parameters
            order by $param/@name
            return concat(encode-for-uri(string($param/@name)),if(string($param/@value))then concat("=",encode-for-uri(string($param/@value)))else ())
            ,"&amp;")
    return
        <http:request method="{$method}"
                      href="{$href}{if($query)then concat("?",$query) else ()}"
                      http-version="1.1"
                      chunked="false">
            <http:header name="x-amz-date" value="{aws-utils:x-amz-date()}" />
            <http:header name="Date" value="{aws-utils:http-date()}" />
        </http:request>
};

(:~
 : Adds the Authorization header to the request according to 
 : <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html">Authenticating Requests (AWS Signature Version 4)</a>
 :  
:)
declare function aws-request:sign_v4(
    $request as element(),
    $bucketname as xs:string,
    $object-key as xs:string,
    $aws-key as xs:string,
    $aws-secret as xs:string) {
    
    let $service := 's3' (: TODO service should be passed in as an argument :)
    let $region := 'us-east-1' (: TODO region should be passed in as an argument :)
    let $nl := '&#10;' (: the newline character :)
    
    let $http-method := $request/@method
    let $href := $request/@href
    let $host := substring-before(substring-after($href, "https://"), "/")
    let $canonical-uri := (if (contains($href, "?")) then substring-before($href, "?") else $href) => substring-after($host)
    let $query-string := substring-after($href, "?")
    let $canonical-query-string := 
        string-join(
            for $param in tokenize($query-string, "&amp;")
            let $name := tokenize($param, "=")[1]
            let $value := tokenize($param, "=")[2]
            order by $name
            return concat($name, "=", $value)
            ,
            "&amp;"
        )

    (: ensure required x-amz-content-sha256 header is present :)
    let $request := 
        if ($request/http:header/@name = "x-amz-content-sha256") then 
            $request
        else
            let $x-amz-content-sha256 := <http:header name="x-amz-content-sha256" value="{crypto:hash(($request/http:body/node(), "")[1], "sha256", "hex")}" />
            return
                (: insert node $x-amz-content-sha256 as first into $request :)
                element { node-name($request) } { $request/@*, $x-amz-content-sha256, $request/* }

    (: ensure required host header is present :)
    let $request := 
        if ($request/http:header/@name = "Host") then 
            $request
        else
            let $host-header := <http:header name="Host" value="{$host}" />
            return
                (: insert node $host-header as first into $request :)
                element { node-name($request) } { $request/@*, $host-header, $request/* }

    let $canonical-headers :=
        let $sorted := 
            for $header in $request/http:header
            let $key := lower-case($header/@name)
            let $value := $header/@value
            order by $key
            return 
                concat(
                    $key,
                    ":",
                    $value,
                    $nl
                )
        return
            string-join($sorted)
            
    let $signed-headers :=
        let $sorted := 
            for $header in $request/http:header
            let $key := lower-case($header/@name)
            order by $key
            return 
                $key
        return
            string-join($sorted, ";")

    let $hashed-payload := $request/http:header[@name eq "x-amz-content-sha256"]/@value
    
    let $canonical-request := concat(   $http-method, $nl,
                                        $canonical-uri, $nl,
                                        $canonical-query-string, $nl,
                                        $canonical-headers,$nl,
                                        $signed-headers, $nl, 
                                        $hashed-payload
                            )

    let $hashed-request := crypto:hash($canonical-request, 'sha256', 'hex')                                  
    
    let $date-YYYYMMDD := aws-utils:yyyymmdd-date()
    let $x-amz-date := $request/http:header[@name eq "x-amz-date"]/@value
    
    let $credential-scope := concat($date-YYYYMMDD, '/', $region, '/', $service, '/aws4_request')

    let $string-to-sign := concat(  'AWS4-HMAC-SHA256', $nl,                        (: Hashing Algorithm :)
                                    $x-amz-date, $nl,                               (: x-amz-date :)
                                    $credential-scope, $nl,                         (: Credential Scope Value :)
                                    $hashed-request                                 (: Hashed Canonical Request :)
                                )
                                
    let $kSecret := concat("AWS4", $aws-secret)
    let $kDate := crypto:hmac($date-YYYYMMDD, $kSecret, "HMAC-SHA-256")
    let $kRegion := crypto:hmac($region, $kDate, "HMAC-SHA-256")
    let $kService := crypto:hmac($service, $kRegion, "HMAC-SHA-256")
    let $kSigning := crypto:hmac("aws4_request", $kService, "HMAC-SHA-256")
    let $signature := crypto:hmac($string-to-sign, $kSigning, "HMAC-SHA-256", "hex")
    
    let $auth-value := concat('AWS4-HMAC-SHA256 Credential=', $aws-key, '/', $date-YYYYMMDD, '/', $region, '/', $service, '/aws4_request,', 
                        'SignedHeaders=', $signed-headers, ',Signature=', $signature)
    
    (:
    let $log := console:log(concat("canonical request: ", $canonical-request))
    let $log := console:log(concat("hashed request: ", $hashed-request))                            
    let $log := console:log(concat("string to sign: ", $string-to-sign))
    let $log := console:log(concat("signature: ", $signature)) 
    let $log := console:log(concat("auth-value: ", $auth-value))                    
    let $log := console:log("url: " || $request/@href)
    :)
    
    let $authorization := <http:header name="Authorization" value= "{$auth-value}"/>
    
    return
        element { node-name($request) } { $request/@*, $authorization, $request/* }
};

(:~
 : Adds the Authorization header to the request according to 
 : <a href="http://docs.amazonwebservices.com/AmazonS3/latest/dev/index.html?RESTAuthentication.html">Amazon S3 RESTAuthentication</a>
 :  
:)
declare function aws-request:sign(
    $request as element(),
    $bucketname as xs:string,
    $object-key as xs:string,
    $aws-key as xs:string,
    $aws-secret as xs:string) {
    let $canonical as xs:string :=
         (: trace( :)
           string-join(
                (
                    (: method :)
                    string($request/@method),"&#10;",
                    
                    (: content-md5 :)
                    string($request/http:header[lower-case(@name)="content-md5"]/@value),"&#10;",
                    
                    (: content-type :)
                    string($request/http:body/@media-type),"&#10;",
                    
                    (: date :)
                    if (not($request/http:header[@name eq "x-amz-date"])) then string($request/http:header[lower-case(@name)="date"]/@value)else (),"&#10;",
                    
                    (: x-amz- headers :)
                    (: @TODO support lists of more than one of the same x-amz-* headers :)
                    for $header in $request/http:header[starts-with(lower-case(string(@name)),"x-amz-")]
                    let $name := lower-case(string($header/@name))
                    let $value := normalize-space(string($header/@value))
                    (:group by $name:)
                    order by $name
                    return 
                        ( $name, ":", string-join($value,","), "&#10;" ),
                        
                    (: add complete key :)
                    "/",
                    if ($bucketname eq "") then () else ($bucketname,"/"),
                    if ($object-key eq "") then () else ($object-key),
                    
                    (: @TODO: add eventually acl, location, logging, versions or torrent parameters from url :)
                    let $href := string($request/@href)
                    let $query := substring-after(string($request/@href),"?")
                    let $key_values := tokenize($query, "&amp;")
                    for $key_value at $count in $key_values[tokenize(.,"=") = ('acl')]
                    let $key := tokenize($key_value,"=")[1]
                    let $value := tokenize($key_value,"=")[2]
                    return 
                        concat(
                            if ($count eq 1) then '?' else '&amp;',
                            $key,
                            if ($value) then concat('=', $value) else ()
                        )
               )
            ,"")
            (:,"canonicalString"):)
    let $signature as xs:string := crypto:hmac($canonical, $aws-secret, "HmacSha1", "base64")
    let $auth-header := <http:header name="Authorization" value="AWS {$aws-key}:{$signature}" />
    return <http:request>{$request/@*, $request/*, $auth-header(:, $canonical:)}</http:request>
};

(:~
 : Adds the Authorization header to the request according to 
 : <a href="http://docs.amazonwebservices.com/AmazonS3/latest/dev/index.html?RESTAuthentication.html">Amazon S3 RESTAuthentication</a>
 :  
:)
(: TODO-eXist :)
(:declare updating function request:sign-v2(
    $request as element(),
    $host as xs:string,
    $path as xs:string,
    $parameters as element(parameter)*,
    $aws-key as xs:string,
    $aws-secret as xs:string) {
    
    request:sign-v2($request,$host,$path,(),$parameters,$aws-key,$aws-secret)
    
};:)

(:~
 : Adds the Authorization header to the request according to 
 : <a href="http://docs.amazonwebservices.com/AmazonS3/latest/dev/index.html?RESTAuthentication.html">Amazon S3 RESTAuthentication</a>
 :  
:)
(: TODO-eXist :)
(:declare updating function request:sign-v2(
    $request as element(),
    $host as xs:string,
    $path as xs:string,
    $version as xs:string?,
    $parameters as element(parameter)*,
    $aws-key as xs:string,
    $aws-secret as xs:string) {
    
    let $date as xs:string := util:timestamp()
    let $signature-parameters :=
        (
            <parameter name="AWSAccessKeyId" value="{$aws-key}" />,
            <parameter name="Timestamp" value="{$date}" />,
            <parameter name="SignatureVersion" value="2" />,
            <parameter name="SignatureMethod" value="HmacSHA1" />,
            if ($version)
            then
                <parameter name="Version" value="{$version}" />
            else ();
        )
    let $canonical as xs:string :=
        (\: trace( :\)
            string-join(
                (
                    (\: method :\)
                    string($request/@method),"&#10;",
                    
                    $host,"&#10;",
                    
                    (\: path :\)
                    $path,"&#10;",
                    
                    (\: parameters :\)
                    string-join(
                        for $param in ($parameters,$signature-parameters)
                        let $name := encode-for-uri(string($param/@name))
                        let $value := encode-for-uri(string($param/@value))
                        order by $name
                        return concat($name, "=",$value),"&amp;")
               )
            ,"")
            (\:,"canonicalString"):\)
    let $signature as xs:string := hmac:sha1($canonical,$aws-secret)
    let $auth-param := <parameter name="Signature" value="{$signature}" />
    let $new-href as xs:string :=
        concat(
            string($request/@href),"&amp;",
            string-join(
                for $param in ($signature-parameters,$auth-param)
                let $name := encode-for-uri(string($param/@name))
                let $value := encode-for-uri(string($param/@value))
                order by $name
                return concat($name, "=",$value),"&amp;"))
    return replace value of node $request/@href with $new-href
};:)

(:~
 : add xml content to an http request
 :
 :
:)
declare function aws-request:add-content-xml($request as element(http:request),$content-xml as item()){

    (: TODO unsure if we really need to serialize before hashing, or if hash serializes? :)
    let $content-string := serialize($content-xml, map { "method": "xml" })
    let $x-amz-content-sha256 := <http:header name="x-amz-content-sha256" value="{crypto:hash($content-string, "sha256", "hex")}" />
    let $body := <http:body media-type="text/xml" method="xml">{$content-xml}</http:body>
    return
        (
            (:insert node $x-amz-content-sha256 as first into $request,:)
            (:insert node $body as last into $request:)
            element { node-name($request) } { $request/@*, $x-amz-content-sha256, $request/*, $body }
        ) 
};

(:~
 : add text content to an http request
 :
 :
:)
declare function aws-request:add-content-text($request as element(http:request),$content-text as xs:string){

    let $x-amz-content-sha256 := <http:header name="x-amz-content-sha256" value="{crypto:hash($content-text, "sha256", "hex")}" />
    let $body := <http:body media-type="text/plain" method="text">{$content-text}</http:body>
    return
        (
            (:insert node $x-amz-content-sha256 as first into $request,:)
            (:insert node $body as last into $request:)
            element { node-name($request) } { $request/@*, $x-amz-content-sha256, $request/*, $body }
        ) 
};

(:~
 : add binary content to an http request
 :
 :
:)
declare function aws-request:add-content-binary(
    $request as element(http:request),$content-binary as xs:base64Binary){

    let $x-amz-content-sha256 := <http:header name="x-amz-content-sha256" value="{crypto:hash($content-binary, "sha256", "hex")}" />
    let $body := <http:body media-type="text/plain" method="binary">{$content-binary}</http:body>
    return
        (
            (:insert node $x-amz-content-sha256 as first into $request,:)
            (:insert node $body as last into $request:)
            element { node-name($request) } { $request/@*, $x-amz-content-sha256, $request/*, $body }
        ) 
};

