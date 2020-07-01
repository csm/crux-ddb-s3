package crux.ddb_s3;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public interface DDBS3Configurator {
    default CreateTableRequest.Builder createTable(CreateTableRequest.Builder builder) {
        return builder;
    }

    default CreateBucketRequest.Builder createBucket(CreateBucketRequest.Builder builder) {
        return builder;
    }

    default PutObjectRequest.Builder putObject(PutObjectRequest.Builder builder) {
        return builder;
    }

    default DynamoDbAsyncClient makeDynamoDBClient() {
        return DynamoDbAsyncClient.create();
    }

    default S3AsyncClient makeS3Client() {
        return S3AsyncClient.create();
    }

    default byte[] freeze(Object value) {
        return NippySerde.freeze(value);
    }

    default Object thaw(byte[] bytes) {
        return NippySerde.thaw(bytes);
    }
}

class NippySerde {
    private static final IFn requiringResolve = Clojure.var("clojure.core/requiring-resolve");
    private static final IFn fastFreeze = (IFn) requiringResolve.invoke(Clojure.read("taoensso.nippy/fast-freeze"));
    private static final IFn fastThaw = (IFn) requiringResolve.invoke(Clojure.read("taoensso.nippy/fast-thaw"));

    static byte[] freeze(Object doc) {
        return (byte[]) fastFreeze.invoke(doc);
    }

    static Object thaw(byte[] bytes) {
        return fastThaw.invoke(bytes);
    }

}
