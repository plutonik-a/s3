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
 :    
 : </p>
 :
 : @author Klaus Wichmann klaus [at] xquery [dot] co [dot] uk
 :)
module namespace bucket = 'http://www.xquery.co.uk/modules/connectors/aws/s3/bucket';

import module namespace http = "http://expath.org/ns/http-client";
import module namespace ser = "http://www.zorba-xquery.com/modules/serialize";
 
import module namespace request = 'http://www.xquery.co.uk/modules/connectors/aws/helpers/request' at '../helpers/request.xq';
import module namespace s3_request = 'http://www.xquery.co.uk/modules/connectors/aws/s3/request' at '../s3/request.xq';
import module namespace utils = 'http://www.xquery.co.uk/modules/connectors/aws/helpers/utils' at '../helpers/utils.xq';
import module namespace error = 'http://www.xquery.co.uk/modules/connectors/aws/s3/error' at '../s3/error.xq';
import module namespace factory = 'http://www.xquery.co.uk/modules/connectors/aws/s3/factory' at '../s3/factory.xq';

(: @todo: this is actually strange that there are two different namespace which are both quite similar :)
declare namespace aws = "http://s3.amazonaws.com/doc/2006-03-01/";
declare namespace s3 = "http://doc.s3.amazonaws.com/2006-03-01";

(:~
 : delete a bucket of a user.
 :
 : Service definition from the Amazon S3 API documentation:
 : <blockquote>"This implementation of the DELETE operation deletes the bucket named in the URI. 
 : All objects (including all object versions and Delete Markers) in the bucket must be deleted 
 : before the bucket itself can be deleted."</blockquote>
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to be deleted 
 : @return returns the http response information (header,statuscode,...) 
:)
declare sequential function bucket:delete(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com")
    let $request := request:create("DELETE",$href)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            (: delete it :)
            s3_request:send($request);
        }
};

(:~
 : list all buckets of a user.
 :
 : Service definition from the Amazon S3 API documentation:
 : <blockquote>"This implementation of the GET operation returns a list of all buckets owned by the authenticated 
 : sender of the request."</blockquote>
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @return returns a pair of 2 items. The first is the http response information; the second is the response document containing
 :         the aws:ListAllMyBucketsResult element 
:)
declare sequential function bucket:list(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string
) as item()* {

    let $href as xs:string := "http://s3.amazonaws.com"
    let $request := request:create("GET",$href)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                "",
                "",
                $aws-access-key,
                $aws-secret);
                
            (: get key list s3 :)
            s3_request:send($request);
        }
};

(:~
 : list objects contained within a bucket.
 : 
 : This convenience function uses the following default values for the <a href="#list-7">list</a> function:
 : <ul>
 :    <li><code>delimiter</code>: None</li>
 :    <li><code>marker</code>: None</li>
 :    <li><code>max-keys</code>: "1000"</li>
 :    <li><code>prefix</code>: None</li>
 : </ul>
 : Therefore, this function can only fetch a maximum of 1000 top object keys (in alphabetical order).
 : For more control and a more detailed description see <a href="#list-7">list</a>.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to list the contained objects
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the main result of this request: a node of a ListBucketResult
:)
declare sequential function bucket:list(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {
    bucket:list($aws-access-key,$aws-secret,$bucket,(),(),(),())
};

(:~
 : list objects contained within a bucket.
 : 
 : Service definition from the Amazon S3 API documentation:
 : <blockquote>"This implementation of the GET operation returns some or all (up to 1000) of the objects in a bucket.
 : You can use the request parameters as selection criteria to return a subset of the objects in a bucket."</blockquote>
 :
 : This function can only fetch a maximum of 1000 object keys (in alphabetical order).
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to be deleted 
 : @param $delimiter the delimiter marks where the listed results stop. For example, a delimiter / lists all objects
 :                   having $prefix plus arbitrary characters except / (default: None) 
 : @param $marker specifies a key as the starting point; following keys in alpabetical order are listed (default: None)
 : @param $max-keys the maximum number of keys returned (default: "1000"). If more keys than $max-key can be fetched, the 
 :                  result contains <IsTruncated>true</IsTruncated>. 
 : @param $prefix only keys starting with the prefix are returned 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the main result of this request: a node of a ListBucketResult
:)
declare sequential function bucket:list(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $delimiter as xs:string?,
    $marker as xs:string?,
    $max-key as xs:string?,
    $prefix as xs:string?
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com")
    let $parameters := 
        (
            if($delimiter) then <parameter name="delimiter" value="{$delimiter}" /> else (),
            if($marker) then <parameter name="marker" value="{$marker}" /> else (),
            if($max-key) then <parameter name="max-key" value="{$max-key}" /> else (),
            if($prefix) then <parameter name="prefix" value="{$prefix}" /> else ()
        )
    let $request := request:create("GET",$href,$parameters)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};

(:~
 : list all object versions contained within a bucket.
 : 
 : This convenience function uses the following default values for the <a href="#list-versions-8">list-versions</a> function:
 : <ul>
 :    <li><code>delimiter</code>: None</li>
 :    <li><code>marker</code>: None</li>
 :    <li><code>max-keys</code>: "1000"</li>
 :    <li><code>prefix</code>: None</li>
 :    <li><code>version-id-marker</code>: None</li>
 : </ul>
 : This function can only fetch a maximum of 1000 top object versions (in alphabetical order).
 : For more control and a more detailed description see <a href="#list-versions-8">list</a>.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to list the contained object versions
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the main result of this request: an s3:ListVersionsResult element
:)
declare sequential function bucket:list-versions(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {
    bucket:list-versions($aws-access-key,$aws-secret,$bucket,(),(),(),(),())
};

(:~
 : list object versions contained within a bucket.
 : 
 : This function can only fetch a maximum of 1000 object keys (in alphabetical order).
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to fetch the object versions from
 : @param $delimiter the delimiter marks where the listed results stop. For example, a delimiter / lists all objects
 :                   having $prefix plus arbitrary characters except / (default: None) 
 : @param $marker specifies a key as the starting point; following keys in alpabetical order are listed (default: None)
 : @param $max-keys the maximum number of keys returned (default: "1000"). If more keys than $max-key can be fetched, the 
 :                  result contains <IsTruncated>true</IsTruncated>. 
 : @param $prefix only keys starting with the prefix are returned
 : @param $version-id-marker the object version to start the listing from 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the main result of this request: an s3:ListVersionsResult element
:)
declare sequential function bucket:list-versions(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $delimiter as xs:string?,
    $marker as xs:string?,
    $max-key as xs:string?,
    $prefix as xs:string?,
    $version-id-marker as xs:string?
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com")
    let $parameters := 
        (
            <parameter name="versions" value="" />,
            if($delimiter) then <parameter name="delimiter" value="{$delimiter}" /> else (),
            if($marker) then <parameter name="marker" value="{$marker}" /> else (),
            if($max-key) then <parameter name="max-key" value="{$max-key}" /> else (),
            if($prefix) then <parameter name="prefix" value="{$prefix}" /> else (),
            if($version-id-marker) then <parameter name="version-id-marker" value="{$version-id-marker}" /> else ()
        )
    let $request := request:create("GET",$href,$parameters)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};

(:~
 : create a bucket for the authenticated user.
 :
 : This is a convenience function for the <a href="#create-5">create</a> function with the following default values:
 : <ul>
 :    <li><code>location</code>: "US"</li>
 :    <li><code>acl</code>: "private"</li>
 : </ul>
 : For more options and a more detailed description see <a href="#create-5">create</a>.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to be deleted 
 : @return returns the http reponse data (headers, statuscode,...)
:)
declare sequential function bucket:create(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {
    bucket:create($aws-access-key,$aws-secret,$bucket,(),())
};

(:~
 : create a bucket for an authenticated user.
 :
 : Service definition from the Amazon S3 API documentation:
 : <blockquote>"This implementation of the PUT operation creates a new bucket. 
 :   To create a bucket, you must register´with Amazon S3 and have a valid AWS Access Key ID to authenticate requests. 
 :   Anonymous requests are never allowed to create buckets. By creating the bucket, you become the bucket owner.
 :   Not every string is an acceptable bucket name. For information on bucket naming restrictions, see 
 :   <a href="http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?UsingBucket.html">Working
 :   with Amazon S3 Buckets</a>."
 : </blockquote>
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to be created 
 : @param $acl optionally, grant access control rights by passing one of the convenience variables <code>$const:ACL-GRANT-...</code>
 :             residing within the <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a> module
 : @param $location optionally, create this bucket in an explicit location by passing one of the convenience variables <code>$const:LOCATION-...</code>
 :             residing within the <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a> module
 : @return returns the http reponse data (headers, statuscode,...)
:)
declare sequential function bucket:create(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $acl as xs:string?,
    $location as xs:string?
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com")
    let $request := request:create("PUT",$href)
    return 
        block{
            (: add location config body and acl header if any :)
            (
                s3_request:add-create-bucket-location($request,$location),
                s3_request:add-acl-everybody($request,$acl)
            );
   
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};

(:~
 : get the access control list (ACL) of a specific bucket. This functions can be used to check granted access
 : rights for this bucket.
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to get the acl from 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the main result of this request: an AccessControlPolicy element
:)
declare sequential function bucket:get-config-acl(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("GET",$href,<parameter name="acl" />)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};

(:~
 : get the policy of a specific bucket. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to get the policy from 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the main result of this request: a JSON string containing the policy data
:)
declare sequential function bucket:get-config-policy(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("GET",$href,<parameter name="policy" />)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};


(:~
 : get the location of a specific bucket. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to get the location from 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the main result of this request: an aws:LocationConstraint element
:)
declare sequential function bucket:get-config-location(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("GET",$href,<parameter name="location" />)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};

(:~
 : get the logging setting of a specific bucket. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to get the logging information from 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the main result of this request: an aws:BucketLoggingStatus element
:)
declare sequential function bucket:get-config-logging(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("GET",$href,<parameter name="logging" />)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};

(:~
 : get the notification setting of a specific bucket. Notifications are important for reduced redundancy storing to send
 : notifications about a lost object. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to get the notification information from 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the main result of this request: an NotificationConfiguration element
:)
declare sequential function bucket:get-config-notification(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("GET",$href,<parameter name="notification" />)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};

(:~
 : get the requestPayment configuration of a specific bucket. The configuration defines who pays for transfer fees of objects within 
 : this bucket (bucket owner or requester).
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to get the requestPayment configuration information from 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the main result of this request: an s3:RequestPaymentConfiguration element
:)
declare sequential function bucket:get-config-request-payment(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("GET",$href,<parameter name="requestPayment" />)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};

(:~
 : get the versioning configuration of a specific bucket. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to get the versioning information from 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the main result of this request: an s3:VersioningConfiguration element
:)
declare sequential function bucket:get-config-versioning(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("GET",$href,<parameter name="versioning" />)
    return 
        block{
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};

(:~
 : grant access rights to a specific bucket for a canonical user or a group of users. This request modifies the existing
 : access control list (ACL) of a bucket. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to grant an access right for
 : @param $grantee User/group identifier to grant access rights to. Can be either a unique AWS user id, an email address of an Amazon customer,
 :                 or a user group identified by a uri (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a>
 :                 for convenience variables <code>$const:ACS-GROUPS...</code>) 
 : @param $permission the permission to be granted to the grantee (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a>
 :                 for convenience variables <code>$const:ACL-GRANT-...</code>)
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the AccessControlPolicy element that has been set for the bucket (contains all granted access rights)
:)
declare sequential function bucket:grant-permission(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $grantee as xs:string,
    $permission as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("PUT",$href,<parameter name="acl" />)
    return 
        block{
            (: get the current acl of the bucket :)
            declare $access-control-policy := bucket:get-config-acl($aws-access-key,$aws-secret,$bucket);
            
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
                "",
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$access-control-policy);
        }
};

(:~
 : remove a granted access right from a specific bucket for a canonical user or a group of users. This request modifies the existing
 : access control list (ACL) of a bucket. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to remove the granted access right from
 : @param $grantee User/group identifier to remove the granted access right from. Can be either a unique AWS user id, an email address 
 :                 of an Amazon customer, or a user group identified by a uri (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a>
 :                 for convenience variables <code>$const:ACS-GROUPS...</code>)
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the AccessControlPolicy element that has been set for the bucket (contains all granted access rights)
:)
declare sequential function bucket:remove-permission(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $grantee as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("PUT",$href,<parameter name="acl" />)
    return 
        block{
            (: get the current acl of the bucket :)
            declare $access-control-policy := bucket:get-config-acl($aws-access-key,$aws-secret,$bucket);
            
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
                "",
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$access-control-policy);
        }
};


(:~
 : Set the policy of a bucket. For more details about bucket policies refer to 
 : <a href="http://docs.amazonwebservices.com/AmazonS3/latest/dev/index.html?UsingBucketPolicies.html" target="_blank">UsingBucketPolicies</a>.
 : The policy is in JSON format and passed to this function as <code>xs:string</code>. 
 : The <a href="http://awspolicygen.s3.amazonaws.com/policygen.html" target="_blank">AWS Policy Generator</a> is 
 : very helpful for generating detailed access policies. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to set the policy for
 : @param $policy the serialized JSON code representing the policy 
 : @return returns the http reponse data (headers, statuscode,...)
:)
declare sequential function bucket:set-policy(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $policy as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("PUT",$href,<parameter name="policy" />)
    return 
        block{
            (: add policy config body :)
            request:add-content-text($request,$policy);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            s3_request:send($request);
        }
};


(:~
 : This function enables logging for a specifig bucket. The logs will be stored in the <code>$logging-bucket</code>
 : or in the monitored <code>$bucket</code> itself if <code>$logging-bucket</code> is empty.
 :
 : You can store the logs of multiple buckets in the same target bucket. Yet, you should provide a <code>$logging-prefix</code>
 : in order to keep the logs distinguishable. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to enable logging for
 : @param $logging-bucket the bucket where the logs will be stored 
 : @param $logging-prefix all logs for this bucket will be stored with this prefix 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the s3:BucketLoggingStatus element that has been set for the bucket logs (contains all granted access rights)
:)
declare sequential function bucket:enable-logging(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $logging-bucket as xs:string?,
    $logging-prefix as xs:string?
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("PUT",$href,<parameter name="logging" />)
    return 
        block{
            (: add logging config body :)
            declare $logging-config := factory:config-enable-bucket-logging($logging-bucket, $logging-prefix);
            s3_request:add-bucket-logging($request,$logging-config);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$logging-config);
        }
};


(:~
 : This function disables logging for a specifig bucket. The logs will no longer be stored.
 :
 : <b>Caution:</b> This removes all granted access rights for the logs. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to enable logging for
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the s3:BucketLoggingStatus element that has been set for the bucket logs
:)
declare sequential function bucket:disable-logging(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("PUT",$href,<parameter name="logging" />)
    return 
        block{
            (: add empty logging config body :)
            declare $logging-config := factory:config-disable-bucket-logging();
            s3_request:add-bucket-logging($request,$logging-config);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$logging-config);
        }
};


(:~
 : grant access rights to the logs of a specific bucket either to a canonical user or a group of users. 
 : This request modifies the existing granted permissions of the logs. 
 : 
 : <b>Caution:</b> This function enables logging if it is currently disabled.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to grant an access right for all of its logs
 : @param $grantee User/group identifier to grant access rights to. Can be either a unique AWS user id, an email address of an Amazon customer,
 :                 or a user group identified by a uri (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a>
 :                 for convenience variables <code>$const:ACS-GROUPS...</code>) 
 : @param $permission the permission to be granted to the grantee (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a>
 :                 for convenience variables <code>$const:ACL-GRANT-...</code>)
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the s3:BucketLoggingStatus element that has been set for the bucket logs (contains all granted access rights)
:)
declare sequential function bucket:grant-logging-permission(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $grantee as xs:string,
    $permission as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("PUT",$href,<parameter name="logging" />)
    return 
        block{
            (: get the current logging config containing all granted permissions of the bucket :)
            declare $logging-config := bucket:get-config-logging($aws-access-key,$aws-secret,$bucket);
            
            (: modify logging config: add or update grant :)
            let $current-grant := 
                $logging-config/s3:BucketLoggingStatus/s3:LoggingEnabled/s3:TargetGrants/s3:Grant
                    [s3:Grantee/s3:ID=$grantee or s3:Grantee/s3:DisplayName=$grantee or s3:Grantee/s3:URI=$grantee]
            return
                if($current-grant)
                then
                    replace value of node $current-grant/s3:Permission with $permission
                else insert node 
                        factory:config-grant($grantee,$permission) 
                     as last into $logging-config/s3:BucketLoggingStatus/s3:LoggingEnabled/s3:TargetGrants; 
                
            (: add logging config body :)
            s3_request:add-bucket-logging($request,$logging-config);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$logging-config);
        }
};


(:~
 : Remove an access right for the logs of a specific bucket either from a canonical user or a group of users. 
 : This request modifies the existing granted permissions of the logs. 
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to remove an access right from for all of its logs
 : @param $grantee User/group identifier to grant access rights to. Can be either a unique AWS user id, an email address of an Amazon customer,
 :                 or a user group identified by a uri (see <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a>
 :                 for convenience variables <code>$const:ACS-GROUPS...</code>) 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the s3:BucketLoggingStatus element that has been set for the bucket logs (contains all granted access rights)
:)
declare sequential function bucket:remove-logging-permission(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $grantee as xs:string
) as item()* {    

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("PUT",$href,<parameter name="logging" />)
    return 
        block{
            (: get the current logging config containing all granted permissions of the bucket :)
            declare $logging-config := bucket:get-config-logging($aws-access-key,$aws-secret,$bucket);
            
            (: if the grantee has been granted an access right remove it :)
            let $current-grant := 
                $logging-config/s3:BucketLoggingStatus/s3:LoggingEnabled/s3:TargetGrants/s3:Grant
                    [s3:Grantee/s3:ID=$grantee or s3:Grantee/s3:DisplayName=$grantee or s3:Grantee/s3:URI=$grantee]
            return
                if($current-grant)
                then
                    delete nodes $current-grant
                else (); 
                
            (: add logging config body :)
            s3_request:add-bucket-logging($request,$logging-config);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$logging-config);
        }
};

(:~
 : This function enables notifications on s3:ReducedRedundancyLostObject events. 
 : Therefore, notification can only be used if the reduced redundancy is turned on. 
 :
 : Please note that AWS will send a notification to the SNS topic to make sure that such a topic 
 : exists. If the passed topic does not exist, you do not have publishing permissions for it, or
 : the topic does not exist in the same region as the bucket, an error:InvalidArgument error is
 : thrown.
 : 
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to enable notifications in the event of a lost object
 : @param $topic the Simple Notification Service (SNS) topic to send the notification to (name starts with "arn:aws:...") 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the NotificationConfiguration element that has been set for a lost object event
:)
declare sequential function bucket:enable-lost-object-notification(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $topic as xs:string
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("PUT",$href,<parameter name="notification" />)
    return 
        block{
            (: add notification config body :)
            declare $config := factory:config-enable-lost-object-notification($topic);
            s3_request:add-bucket-notification($request,$config);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$config);
        }
};


(:~
 : This function disables notifications on s3:ReducedRedundancyLostObject events. 
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to disable notifications in the event of a lost object
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the NotificationConfiguration element that has been set for a lost object event
:)
declare sequential function bucket:disable-lost-object-notification(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("PUT",$href,<parameter name="notification" />)
    return 
        block{
            (: add notification config body :)
            declare $config := factory:config-disable-lost-object-notification();
            s3_request:add-bucket-notification($request,$config);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$config);
        }
};


(:~
 : This function lets you configure weither the requester or the owner of a bucket pays for request and 
 : data transfer cost.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to configure the payment settings for
 : @param $payer a string defining either the owner of a bucket or the requester (use variables from 
 :               <a href="http://www.xquery.co.uk/modules/connectors/aws/s3/constants">constants</a> for convenience 
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the RequestPaymentConfiguration element that has been set for the bucket
:)
declare sequential function bucket:set-request-payment-configuration(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $payer as xs:string
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("PUT",$href,<parameter name="requestPayment" />)
    return 
        block{
            (: add request payment config body :)
            declare $config := factory:config-request-payment($payer);
            s3_request:add-bucket-request-payment-config($request,$config);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$config);
        }
};

(:~
 : This function enables versioning for objects contained in the bucket. 
 : 
 : If MfaDelete is enabled for the bucket you can only change versioning settings with your Mfa device.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to enable versioning for.
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the VersioningConfiguration element that has been set for the bucket
:)
declare sequential function bucket:enable-versioning(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("PUT",$href,<parameter name="versioning" />)
    return 
        block{
            (: add versioning config body :)
            declare $config := factory:config-enable-versioning();
            s3_request:add-bucket-versioning-config($request,$config);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$config);
        }
};

(:~
 : This function enables versioning for objects contained in the bucket. 
 : 
 : In the same request mfa-deletion can be enabled or disabled. If mfa-delete is set to true objects
 : won't be deleted permanently any more. Instead they will only be marked as deleted.
 :
 : If MfaDelete is already enabled or you enabel mfa-delete for the bucket you will not be able to change 
 : versioning settings without the Mfa device.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to enable versioning for.
 : @param $mfa-delete if set to true objects within the bucket will not be deleted anymore but only flagged as deleted.
 :                    if you don't know what an MFA-device is set this option to false. Otherwise you won't be able to 
 :                    delete any objects from the bucket any more.  
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the VersioningConfiguration element that has been set for the bucket
:)
declare sequential function bucket:enable-versioning(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $mfa-delete as xs:boolean
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("PUT",$href,<parameter name="versioning" />)
    return 
        block{
            (: add versioning config body :)
            declare $config := factory:config-enable-versioning($mfa-delete);
            s3_request:add-bucket-versioning-config($request,$config);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$config);
        }
};

(:~
 : This function disables versioning for objects contained in the bucket. 
 :
 : If MfaDelete is enabled for the bucket you can only change versioning settings with your Mfa device.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to enable versioning for.
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the VersioningConfiguration element that has been set for the bucket
:)
declare sequential function bucket:disable-versioning(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("PUT",$href,<parameter name="versioning" />)
    return 
        block{
            (: add versioning config body :)
            declare $config := factory:config-disable-versioning();
            s3_request:add-bucket-versioning-config($request,$config);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$config);
        }
};

(:~
 : This function disables versioning for objects contained in the bucket. 
 :
 : In the same request mfa-deletion can be enabled or disabled. If mfa-delete is set to true objects
 : won't be deleted permanently any more. Instead they will only be marked as deleted.
 :
 : If MfaDelete is already enabled or you enable mfa-delete for the bucket you will not be able to change 
 : versioning settings without the Mfa device.
 :
 : @param $aws-access-key Your personal "AWS Access Key" that you can get from your amazon account 
 : @param $aws-secret Your personal "AWS Secret" that you can get from your amazon account
 : @param $bucket the name of the bucket to enable versioning for.
 : @param $mfa-delete if set to true objects within the bucket will not be deleted anymore but only flagged as deleted.
 :                    if you don't know what an MFA-device is set this option to false. Otherwise you won't be able to 
 :                    delete any objects from the bucket any more.  
 : @return returns a pair of two items, the first is the http reponse data (headers, statuscode,...), the second
 :         is the VersioningConfiguration element that has been set for the bucket
:)
declare sequential function bucket:disable-versioning(
    $aws-access-key as xs:string, 
    $aws-secret as xs:string,
    $bucket as xs:string,
    $mfa-delete as xs:boolean
) as item()* {

    let $href as xs:string := concat("http://", $bucket, ".s3.amazonaws.com/")
    let $request := request:create("PUT",$href,<parameter name="versioning" />)
    return 
        block{
            (: add versioning config body :)
            declare $config := factory:config-disable-versioning($mfa-delete);
            s3_request:add-bucket-versioning-config($request,$config);
            
            (: sign the request :)
            request:sign(
                $request,
                $bucket,
                "",
                $aws-access-key,
                $aws-secret);
                
            (s3_request:send($request),$config);
        }
};
