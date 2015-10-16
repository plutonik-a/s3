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
module namespace response = 'http://www.xquery.co.uk/modules/connectors/aws/helpers/response';

import module namespace http = "http://expath.org/ns/http-client";
import module namespace ser = "http://www.zorba-xquery.com/modules/serialize";
import module namespace hash = "http://www.zorba-xquery.com/modules/security/hash";
import module namespace base64 = "http://www.zorba-xquery.com/modules/base64";

(:~
 : get etag header
 :
 : @param $response the response of a http request to amazon
 : @return etag as xs:string
:)
declare function response:etag($response as item()*) as xs:string? {

    let $etag :=
        $response[1]//http:header[lower-case(string(@name))="etag"]/@value
    return
        if($etag)
        then string($etag)
        else ()
};

(:~
 : get a particular metadata from an amazon http response by metadata name 
 :
 : @param $response the response of a http request to amazon
 : @param $name the name of the metadata
 : @return metadata values if any
:)
declare function response:metadata($response as item()*, $name as xs:string) as xs:string* {

    let $header := $response[1]/http:header[string(@name)=concat("x-amz-meta-",$name)]
    let $values := tokenize(string($header/@value),",")
    return
        $values
};

(:~
 : get all metadata from an amazon http response wrapped in a metadata element container 
 :
 : @param $response the response of a http request to amazon
 : @return metadata values in a metadata XML element
:)
declare function response:metadata($response as item()*) as element(metadata) {

    let $metadata := 
        <metadata>{
            (: get version id if any :)
            let $version := $response[1]/http:header[string(@name)="x-amz-version-id"]
            return if($version) then attribute version { string($version/@value) } else (),
            
            (: missing metadata elements; some soap metadata can not be returned because it
               is an invalid http header; get the number of those items if any :) 
            let $missing := $response[1]/http:header[string(@name)="x-amz-missing-meta"]
            return if($missing) then attribute missing { string($missing/@value) } else (),
            
            (: all custom user metadata :)
            for $header in $response[1]/http:header[starts-with(string(@name),"x-amz-meta-")]
            let $name := substring-after(string($header/@name),"x-amz-meta-")
            let $values := tokenize(string($header/@value),",")
            for $value in $values
            return element { $name } { $value }
        }</metadata>
    return
        $metadata
};

