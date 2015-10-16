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
module namespace object = 'http://www.xquery.co.uk/modules/connectors/aws/s3/object';

import module namespace http = "http://expath.org/ns/http-client";
import module namespace ser = "http://www.zorba-xquery.com/modules/serialize";

import module namespace request = 'http://www.xquery.co.uk/modules/connectors/aws/helpers/request' at '../helpers/request.xq';
import module namespace s3_request = 'http://www.xquery.co.uk/modules/connectors/aws/s3/request' at '../s3/request.xq';
import module namespace error = 'http://www.xquery.co.uk/modules/connectors/aws/s3/error' at 'error.xq';
import module namespace response = 'http://www.xquery.co.uk/modules/connectors/aws/helpers/response' at '../helpers/response.xq';
import module namespace factory = 'http://www.xquery.co.uk/modules/connectors/aws/s3/factory' at '../s3/factory.xq';

(:~ 
 : delete an object from a bucket of a user. The user is authenticated with the aws-access-key and aws-secret.
:)
declare variable $object:test as xs:string := "value";
(:~ 
 : delete an object from a bucket of a user. The user is authenticated with the aws-access-key and aws-secret.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket containing the object to be deleted 
 : @param $key the key of the object to be deleted
 : @return returns the http-response information (header, statuscode,...) 
:)
declare sequential function object:delete(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/",$key)
    let $request := request:create("DELETE",$href)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            (: delete it :)
            s3_request:send($request);
        }
};

(:~ 
 : delete specific versoin of an object from a bucket of a user. The user is authenticated with the aws-access-key 
 : and aws-secret. If mfa-deletion is enabled you will not be able to delete an object without your MFA device.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket containing the object to be deleted 
 : @param $key the key of the object to be deleted
 : @param $version-id the version id of the object to delete 
 : @return returns the http-response information (header, statuscode,...) 
:)
declare sequential function object:delete(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string,
    $version-id as xs:string
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/",$key)
    let $request := request:create("DELETE",$href,<parameter name="versionId" value="{$version-id}" />)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            (: delete it :)
            s3_request:send($request);
        }
};

(:~
 : get an s3 object.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket containing the object to be retrieved 
 : @param $key the key of the object to be retrieved 
 : @return returns a pair of 2 items. The first is the http response information (header, statuscode, ...); the second is the 
 :         object's data as xml, string (text), or xs:base64Binary
:)
declare sequential function object:read(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/", $key)
    let $request := request:create("GET",$href)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            (: get object from s3 :)
            s3_request:send($request);

        }
};

(:~
 : get a specific version of an s3 object. Versioning needs to be enabled for the containing bucket.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket containing the object to be retrieved 
 : @param $key the key of the object to be retrieved 
 : @param $version-id the version id of the object to read 
 : @return returns a pair of 2 items. The first is the http response information (header, statuscode, ...); the second is the 
 :         object's data as xml, string (text), or xs:base64Binary
:)
declare sequential function object:read(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string,
    $version-id as xs:string
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/", $key)
    let $request := request:create("GET",$href,<parameter name="versionId" value="{$version-id}" />)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            (: get object from s3 :)
            s3_request:send($request);

        }
};

(:~
 : get the access control list (ACL) of an object. This functions can be used to check granted access
 : rights for this object.
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket in which the object is stored 
 : @param $key the key of the object to get the acl from 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the main result of this request: an AccessControlPolicy element
:)
declare sequential function object:get-config-acl(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/", $key)
    let $request := request:create("GET",$href,<parameter name="acl" />)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};

(:~
 : get the access control list (ACL) of an particular version of an object. This functions can be used to 
 : check granted access rights for this object version if object versioning is enabled.
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket in which the object is stored 
 : @param $key the key of the object to get the acl from 
 : @param $version-id the version id of the object to get the acl from 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the main result of this request: an AccessControlPolicy element
:)
declare sequential function object:get-config-acl(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string,
    $version-id as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/", $key)
    let $request := request:create("GET",$href,
            (
                <parameter name="versionId" value="{$version-id}" />,
                <parameter name="acl" />
            )
        )
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};

(:~
 : get the torrent file of an object.
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket in which the object is stored 
 : @param $key the key of the object to get the torrent specification for 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         item is the base64 encoded torrent file of the object
:)
declare sequential function object:get-config-torrent(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/", $key)
    let $request := request:create("GET",$href,<parameter name="torrent" />)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};

(:~
 : get metadata of an s3 object. This effectively sends a HEAD http request to s3. If you only wish to get
 : the metadata of an s3 object exclusively without retrieving the objects data, then this is the right function to 
 : use. If you want to read both the data of the object AND the metadata, then it is more efficient to
 : use the object:read#4 function. The response will also contain the metadata that can be extracted by using the 
 : <code>response:metadata($request)</code> function.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket containing the s3 object 
 : @param $key the key of the s3 object to get the metadata from 
 : @return returns the http reponse data (headers, statuscode,...) that contains the metadata
:)
declare sequential function object:metadata(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/", $key)
    let $request := request:create("HEAD",$href)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            (: get metadata from s3 :)
            s3_request:send($request);
        }
};

(:~
 : get metadata of a specific version of an s3 object. Versioning needs to be enabled for the containing bucket.
 :
 : This function effectively sends a HEAD http request to s3. If you only wish to get
 : the metadata of an s3 object exclusively without retrieving the objects data, then this is the right function to 
 : use. If you want to read both the data of the object AND the metadata, then it is more efficient to
 : use the object:read#4 function. The response will also contain the metadata that can be extracted by using the 
 : <code>response:metadata($request)</code> function.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket containing the s3 object 
 : @param $key the key of the s3 object to get the metadata from 
 : @param $version-id the version id of the object to get the metadata from 
 : @return returns the http reponse data (headers, statuscode,...) that contains the metadata
:)
declare sequential function object:metadata(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string,
    $version-id as xs:string
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/", $key)
    let $request := request:create("HEAD",$href,<parameter name="versionId" value="{$version-id}" />)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            (: get metadata from s3 :)
            s3_request:send($request);
        }
};

(:~
 : upload xml (<code>node()</code> or <code>document-node()</code>), text (<code>xs:string</code>), or binary data 
 : (<code>xs:base64Binary</code>) content into an s3 object. The uploaded object will be marked as "private" which means 
 : it is not publicly accessible. 
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket into which the object is uploaded 
 : @param $key a key for the object into which the uploaded data will be stored
 : @return returns the http reponse data (headers, statuscode,...)
:)
declare sequential function object:write(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string,
    $content as item()
) as item()* {

    object:write($aws-access-key,$aws-secret,$bucket,$key,$content,(),(),())
};

(:~
 : upload xml (<code>node()</code> or <code>document-node()</code>), text (<code>xs:string</code>), or binary data 
 : (<code>xs:base64Binary</code>) content into an s3 object. The uploaded object will be marked as "private" which means 
 : it is not publicly accessible. In addition, you can attach any metadata to the object. For example:
 :
 : <pre>
 :   <code>
 :   <![CDATA[
 : <metadata>
 :    <author>Jon</author>
 :    <author>Jane</author>
 :    <category>XQuery</category>
 : </metadata>
 :   ]]>
 :   </code>
 : </pre>
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket into which the object is uploaded 
 : @param $key a key for the object into which the uploaded data will be stored
 : @param $metadata optionally, you can add any custom metadata to the uploaded object
 : @return returns the http reponse data (headers, statuscode,...)
:)
declare sequential function object:write(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string,
    $content as item(),
    $metadata as element(metadata)
) as item()* {

    object:write($aws-access-key,$aws-secret,$bucket,$key,$content,$metadata,())
};


(:~
 : upload xml (<code>node()</code> or <code>document-node()</code>), text (<code>xs:string</code>), or binary data 
 : (<code>xs:base64Binary</code>) content into an s3 object. The uploaded object will be marked as "private" which means 
 : it is not publicly accessible. In addition, you can attach any metadata to the object. For example:
 :
 : <pre>
 :   <code>
 :   <![CDATA[
 : <metadata>
 :    <author>Jon</author>
 :    <author>Jane</author>
 :    <category>XQuery</category>
 : </metadata>
 :   ]]>
 :   </code>
 : </pre>
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket into which the object is uploaded 
 : @param $key a key for the object into which the uploaded data will be stored
 : @param $metadata optionally, you can add any custom metadata to the uploaded object
 : @param $reduced-redundancy optionally, you can store any data with reduced redundancy to save cost.
 :                            You should do this only for uncritical reproducable data. Per default 
 :                            $reduced-redundancy is turned off.
 : @return returns the http reponse data (headers, statuscode,...)
:)
declare sequential function object:write(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string,
    $content as item(),
    $metadata as element(metadata)?,
    $reduced-redundancy as xs:boolean?
) as item()* {

    object:write($aws-access-key,$aws-secret,$bucket,$key,$content,$metadata,(),$reduced-redundancy)
};

(:~
 : upload xml (<code>node()</code> or <code>document-node()</code>), text (<code>xs:string</code>), or binary data 
 : (<code>xs:base64Binary</code>) content into an s3 object. Additionally, you can grant acl to extent accessibility (By
 : default the object is marked as "private" which means it is not publicly accessible). Also, you can attach any metadata
 : to the object. For example:
 :
 : <pre>
 :   <code>
 :   <![CDATA[
 : <metadata>
 :    <author>Jon</author>
 :    <author>Jane</author>
 :    <category>XQuery</category>
 : </metadata>
 :   ]]>
 :   </code>
 : </pre>
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket into which the object is uploaded 
 : @param $key a key for the object into which the uploaded data will be stored
 : @param $metadata optionally, you can add any custom metadata to the uploaded object
 : @param $acl optionally, grant access control rights by passing one of the convenience variables <code>$const:ACL-GRANT-...</code>
 :             residing within the <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a> module
 : @param $reduced-redundancy optionally, you can store any data with reduced redundancy to save cost.
 :                            You should do this only for uncritical reproducable data. Per default 
 :                            $reduced-redundancy is turned off.
 : @return returns the http reponse data (headers, statuscode,...)
:)
declare sequential function object:write(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string,
    $content as item(),
    $metadata as element(metadata)?,
    $acl as xs:string?,
    $reduced-redundancy as xs:boolean?
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/", $key)
    let $request := request:create("PUT",$href)
    return 
        block{
            (: add body and content-length header and acl header :)
            (
                typeswitch ($content)
                    case $c as xs:string return request:add-content-text($request,$c)
                    case $c as text() return request:add-content-text($request,$c)
                    case $c as element() return request:add-content-xml($request,$c)
                    case $c as document-node() return request:add-content-xml($request,$c)
                    case $c as xs:base64Binary return request:add-content-binary($request,$c)
                    default $c return error(
                        xs:QName("error:S3_UNSUPPORTED_PUT_CONTENT"),
                        "The provided content is not supported. Only xs:string,text,element,document-node,xs:base64Binary is allowed.",
                        $c)
                ,
                s3_request:add-acl-everybody($request,$acl),
                if ($metadata) then s3_request:add-metadata($request,$metadata/*) else (),
                if($reduced-redundancy)then s3_request:add-reduced-redundancy($request) else ()
            );
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            (: upload to s3 :)
            s3_request:send($request);
        }
};


(:~
 : grant access rights to a specific object for everybody. This request replaces existing
 : access control list (ACL) of this object.
 : 
 : If versioning is enabled for that object this functions sets permissions for the latest version of the object. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket in which the object resides to set the access right for everybody
 : @param $key the key of the object to set the permission for everybody
 : @param $acl the permission to be granted to everybody (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a>
 :                 for convenience variables <code>$const:ACL-GRANT-...</code>)
 : @return returns the http reponse data (headers, statuscode,...)
:)
declare sequential function object:set-acl(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string,
    $acl as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/", $key)
    let $request := request:create("PUT",$href,<parameter name="acl" />)
    return 
        block{
            (: set the acl header of the object :)
            s3_request:add-acl-everybody($request,$acl);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};

(:~
 : grant access rights to a specific object for a canonical user or a group of users. This request modifies the existing
 : access control list (ACL) of an object.
 : 
 : If versioning is enabled for that object this functions grants permissions for the latest version of the object. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket in which the object resides to grant an access right for
 : @param $key a key for the object to grant a permission for
 : @param $grantee User/group identifier to grant access rights to. Can be either a unique AWS user id, an email address of an Amazon customer,
 :                 or a user group identified by a uri (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a> 
 :                 for convenience variables <code>$const:ACS-GROUPS...</code>) 
 : @param $permission the permission to be granted to the grantee (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a>
 :                 for convenience variables <code>$const:ACL-GRANT-...</code>)
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the AccessControlPolicy element that has been set for the object (contains all granted access rights)
:)
declare sequential function object:grant-permission(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string,
    $grantee as xs:string,
    $permission as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/", $key)
    let $request := request:create("PUT",$href,<parameter name="acl" />)
    return 
        block{
            (: get the current acl of the object :)
            declare $access-control-policy := object:get-config-acl($aws-access-key,$aws-secret,$bucket,$key);
            
            (: modify policy: add or update grant :)
            let $current-grant := 
                $access-control-policy/AccessControlPolicy/AccessControlList/Grant
                    [Grantee/ID=$grantee or Grantee/DisplayName=$grantee or Grantee/URI=$grantee]
            return
                if($current-grant)
                then
                    replace value of node $current-grant/Permission with $permission
                else insert node 
                        factory:config-grant($grantee,$permission) 
                     as last into $access-control-policy/AccessControlPolicy/AccessControlList; 
                
            (: add acl config body :)
            s3_request:add-acl-grantee($request,$access-control-policy);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$access-control-policy);
        }
};

(:~
 : Grant access rights to a specific version of an object for a canonical user or a group of users. This request modifies the existing
 : access control list (ACL) of that version of the object.
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket in which the object resides to grant an access right for
 : @param $key the key for the object to grant a permission for
 : @param $grantee User/group identifier to grant access rights to. Can be either a unique AWS user id, an email address of an Amazon customer,
 :                 or a user group identified by a uri (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a> 
 :                 for convenience variables <code>$const:ACS-GROUPS...</code>) 
 : @param $permission the permission to be granted to the grantee (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a>
 :                 for convenience variables <code>$const:ACL-GRANT-...</code>)
 : @param $version-id the version id of the object to delete 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the AccessControlPolicy element that has been set for the object version (contains all granted access rights)
:)
declare sequential function object:grant-permission(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string,
    $grantee as xs:string,
    $permission as xs:string,
    $version-id as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/", $key)
    let $request := request:create(
                                    "PUT",
                                    $href,
                                    (<parameter name="acl" />,<parameter name="versionId" value="{$version-id}" />)
                                  )
    return 
        block{
            (: get the current acl of the object version :)
            declare $access-control-policy := object:get-config-acl($aws-access-key,$aws-secret,$bucket,$key,$version-id);
            
            (: modify policy: add or update grant :)
            let $current-grant := 
                $access-control-policy/AccessControlPolicy/AccessControlList/Grant
                    [Grantee/ID=$grantee or Grantee/DisplayName=$grantee or Grantee/URI=$grantee]
            return
                if($current-grant)
                then
                    replace value of node $current-grant/Permission with $permission
                else insert node 
                        factory:config-grant($grantee,$permission) 
                     as last into $access-control-policy/AccessControlPolicy/AccessControlList; 
                
            (: add acl config body :)
            s3_request:add-acl-grantee($request,$access-control-policy);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$access-control-policy);
        }
};


(:~
 : Remove a granted access right from a specific object for a canonical user or a group of users. This request modifies the existing
 : access control list (ACL) of an object. 
 : 
 : If versioning is enabled for the object this functions removes a permission from the latest version of the object. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket in which the object resides to remove the granted access right from
 : @param $key the key of the object to remove a permission from
 : @param $grantee User/group identifier to remove the granted access right from. Can be either a unique AWS user id, an email address 
 :                 of an Amazon customer, or a user group identified by a uri (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a>
 :                 for convenience variables <code>$const:ACS-GROUPS...</code>)
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the AccessControlPolicy element that has been set for the bucket (contains all granted access rights)
:)
declare sequential function object:remove-permission(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string,
    $grantee as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/",$key)
    let $request := request:create("PUT",$href,<parameter name="acl" />)
    return 
        block{
            (: get the current acl of the object :)
            declare $access-control-policy := object:get-config-acl($aws-access-key,$aws-secret,$bucket,$key);
            
            (: modify policy: remove grant :)
            let $current-grant := 
                $access-control-policy/AccessControlPolicy/AccessControlList/Grant
                    [Grantee/ID=grantee or Grantee/DisplayName=grantee or Grantee/URI=grantee]
            return
                if($current-grant)
                then
                    delete node $current-grant
                else (); 
                
            (: add acl config body :)
            s3_request:add-acl-grantee($request,$access-control-policy);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$access-control-policy);
        }
};


(:~
 : Remove a granted access right from a specific object for a canonical user or a group of users. This request modifies the existing
 : access control list (ACL) of an object. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket in which the object resides to remove the granted access right from
 : @param $key the key of the object to remove a permission from
 : @param $grantee User/group identifier to remove the granted access right from. Can be either a unique AWS user id, an email address 
 :                 of an Amazon customer, or a user group identified by a uri (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a>
 :                 for convenience variables <code>$const:ACS-GROUPS...</code>)
 : @param $version-id the version id of the object to delete 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the AccessControlPolicy element that has been set for the bucket (contains all granted access rights)
:)
declare sequential function object:remove-permission(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $key as xs:string,
    $grantee as xs:string,
    $version-id as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/",$key)
    let $request := request:create(
                                    "PUT",
                                    $href,
                                    (<parameter name="acl" />,<parameter name="versionId" value="{$version-id}" />)
                                  )
    return 
        block{
            (: get the current acl of the object version :)
            declare $access-control-policy := object:get-config-acl($aws-access-key,$aws-secret,$bucket,$key,$version-id);
            
            (: modify policy: remove grant :)
            let $current-grant := 
                $access-control-policy/AccessControlPolicy/AccessControlList/Grant
                    [Grantee/ID=grantee or Grantee/DisplayName=grantee or Grantee/URI=grantee]
            return
                if($current-grant)
                then
                    delete node $current-grant
                else (); 
                
            (: add acl config body :)
            s3_request:add-acl-grantee($request,$access-control-policy);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                $key,
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$access-control-policy);
        }
};

(:~
 : Copy an object that is already stored on s3 into a different object in the same bucket in which the source object resides. 
 : The access rights of the source object are not copied with this function. By default, this function sets the target object 
 : access right to private. 
 : 
 : If versioning is enabled for that object this functions copies the latest version of the object. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket from which an object is to be copied and into that the target object is written
 : @param $source-key the key of the object to copy from
 : @param $target-key the key of the object to copy to
 : @return returns the http reponse data (headers, statuscode,...)
:)
declare sequential function object:copy(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $source-key as xs:string,
    $target-key as xs:string
) as item()* {    
    object:copy(
        $aws-access-key, 
        $aws-secret,
        $bucket,
        $source-key,
        $bucket,
        $target-key,
        (),
        (),
        ()
    )
};

(:~
 : Copy an object that is already stored on s3 into a target bucket. The access rights of the source object are
 : not copied with this function. By default, this function sets the target object access right to private. 
 : 
 : If versioning is enabled for that object this functions copies the latest version of the object. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $source-bucket the name of the bucket from which an object is to be copied
 : @param $source-key the key of the object to copy from
 : @param $target-bucket the name of the bucket to copy the source-object to
 : @param $target-key the key of the object to copy to
 : @return returns the http reponse data (headers, statuscode,...)
:)
declare sequential function object:copy(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $source-bucket as xs:string,
    $source-key as xs:string,
    $target-bucket as xs:string,
    $target-key as xs:string
) as item()* {    
    object:copy(
        $aws-access-key, 
        $aws-secret,
        $source-bucket,
        $source-key,
        $target-bucket,
        $target-key,
        (),
        (),
        ()
    )
};

(:~
 : Copy an object that is already stored on s3 into a target bucket. The access rights of the source object are
 : not copied with this function. By default, this function sets the target object access right to private. 
 : 
 : If versioning is enabled for that object this functions copies the latest version of the object. 
 : 
 : Additionaly, you can provide any metadata to replace the source object's metadata with in the target object. For example:
 :
 : <pre>
 :   <code>
 :   <![CDATA[
 : <metadata>
 :    <author>Jon</author>
 :    <author>Jane</author>
 :    <category>XQuery</category>
 : </metadata>
 :   ]]>
 :   </code>
 : </pre>
 : 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $source-bucket the name of the bucket from which an object is to be copied
 : @param $source-key the key of the object to copy from
 : @param $target-bucket the name of the bucket to copy the source-object to
 : @param $target-key the key of the object to copy to
 : @param $acl the permission to be granted to everybody (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a>
 :                 for convenience variables <code>$const:ACL-GRANT-...</code>) for the target object
 : @param $metadata optionally, you can provide any custom metadata to the target object of the copy function. If $metadata parameter
 :                  is an empty sequence then the source object's metadata is copied. In order to simply replace the source object metadata
 :                  with no metadata pass an empty <code>&lt;metadata /></code> element
 : @param $reduced-redundancy optionally, you can store any data with reduced redundancy to save cost.
 :                            You should do this only for uncritical reproducable data. Per default 
 :                            $reduced-redundancy is turned off.
 : @return returns the http reponse data (headers, statuscode,...)
:)
declare sequential function object:copy(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $source-bucket as xs:string,
    $source-key as xs:string,
    $target-bucket as xs:string,
    $target-key as xs:string,
    $acl as xs:string?,
    $metadata as element(metadata)?,
    $reduced-redundancy as xs:boolean?
) as item()* {    

    object:copy(
        $aws-access-key, 
        $aws-secret,
        $source-bucket,
        $source-key,
        $target-bucket,
        $target-key,
        (),
        (),
        (),
        ()
    )
};


(:~
 : Copy a specific version of an object that is already stored on s3 into a target bucket. The access rights of the source object are
 : not copied with this function. By default, this function sets the target object access right to private. 
 : 
 : Additionaly, you can provide any metadata to replace the source object's metadata with in the target object. For example:
 :
 : <pre>
 :   <code>
 :   <![CDATA[
 : <metadata>
 :    <author>Jon</author>
 :    <author>Jane</author>
 :    <category>XQuery</category>
 : </metadata>
 :   ]]>
 :   </code>
 : </pre>
 : 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $source-bucket the name of the bucket from which an object is to be copied
 : @param $source-key the key of the object to copy from
 : @param $target-bucket the name of the bucket to copy the source-object to
 : @param $target-key the key of the object to copy to
 : @param $acl the permission to be granted to everybody (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a>
 :                 for convenience variables <code>$const:ACL-GRANT-...</code>) for the target object
 : @param $metadata optionally, you can provide any custom metadata to the target object of the copy function. If $metadata parameter
 :                  is an empty sequence then the source object's metadata is copied. In order to simply replace the source object metadata
 :                  with no metadata pass an empty <code>&lt;metadata /></code> element
 : @param $reduced-redundancy optionally, you can store any data with reduced redundancy to save cost.
 :                            You should do this only for uncritical reproducable data. Per default 
 :                            $reduced-redundancy is turned off.
 : @param $version-id the version id of the source object to be copied 
 : @return returns the http reponse data (headers, statuscode,...)
:)
declare sequential function object:copy(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $source-bucket as xs:string,
    $source-key as xs:string,
    $target-bucket as xs:string,
    $target-key as xs:string,
    $acl as xs:string?,
    $metadata as element(metadata)?,
    $reduced-redundancy as xs:boolean?,
    $version-id as xs:string?
) as item()* {    

    let $href as xs:string := concat("http://", $target-bucket, ".s3.amazonaws.com/", $target-key)
    let $request := 
        if ($version-id) 
        then
            request:create("PUT",$href,<parameter name="versionId" value="{$version-id}" />)
        else
            request:create("PUT",$href)
    return 
        block{
            (
                s3_request:add-acl-everybody($request,$acl),
                s3_request:add-metadata($request,$metadata/*),
                s3_request:add-copy-source($request,$source-bucket,$source-key),
                if($metadata) then s3_request:add-replace-metadata-flag($request) else (),
                if($reduced-redundancy)then s3_request:add-reduced-redundancy($request) else ()
            );
                
            (: sign the request :)
            request:sign(
                $request,
                $target-bucket,
                $target-key,
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};
