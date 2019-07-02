package io.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.18.0)",
    comments = "Source: unique_key_query.proto")
public final class UniKeyQueSrvGrpc {

  private UniKeyQueSrvGrpc() {}

  public static final String SERVICE_NAME = "UniKeyQueSrv";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.grpc.UniKeyQueReq,
      io.grpc.UniKeyQueResp> getQueryUniqueKeyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "queryUniqueKey",
      requestType = io.grpc.UniKeyQueReq.class,
      responseType = io.grpc.UniKeyQueResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.grpc.UniKeyQueReq,
      io.grpc.UniKeyQueResp> getQueryUniqueKeyMethod() {
    io.grpc.MethodDescriptor<io.grpc.UniKeyQueReq, io.grpc.UniKeyQueResp> getQueryUniqueKeyMethod;
    if ((getQueryUniqueKeyMethod = UniKeyQueSrvGrpc.getQueryUniqueKeyMethod) == null) {
      synchronized (UniKeyQueSrvGrpc.class) {
        if ((getQueryUniqueKeyMethod = UniKeyQueSrvGrpc.getQueryUniqueKeyMethod) == null) {
          UniKeyQueSrvGrpc.getQueryUniqueKeyMethod = getQueryUniqueKeyMethod = 
              io.grpc.MethodDescriptor.<io.grpc.UniKeyQueReq, io.grpc.UniKeyQueResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "UniKeyQueSrv", "queryUniqueKey"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.grpc.UniKeyQueReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.grpc.UniKeyQueResp.getDefaultInstance()))
                  .setSchemaDescriptor(new UniKeyQueSrvMethodDescriptorSupplier("queryUniqueKey"))
                  .build();
          }
        }
     }
     return getQueryUniqueKeyMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static UniKeyQueSrvStub newStub(io.grpc.Channel channel) {
    return new UniKeyQueSrvStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static UniKeyQueSrvBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new UniKeyQueSrvBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static UniKeyQueSrvFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new UniKeyQueSrvFutureStub(channel);
  }

  /**
   */
  public static abstract class UniKeyQueSrvImplBase implements io.grpc.BindableService {

    /**
     */
    public void queryUniqueKey(io.grpc.UniKeyQueReq request,
        io.grpc.stub.StreamObserver<io.grpc.UniKeyQueResp> responseObserver) {
      asyncUnimplementedUnaryCall(getQueryUniqueKeyMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getQueryUniqueKeyMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                io.grpc.UniKeyQueReq,
                io.grpc.UniKeyQueResp>(
                  this, METHODID_QUERY_UNIQUE_KEY)))
          .build();
    }
  }

  /**
   */
  public static final class UniKeyQueSrvStub extends io.grpc.stub.AbstractStub<UniKeyQueSrvStub> {
    private UniKeyQueSrvStub(io.grpc.Channel channel) {
      super(channel);
    }

    private UniKeyQueSrvStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected UniKeyQueSrvStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new UniKeyQueSrvStub(channel, callOptions);
    }

    /**
     */
    public void queryUniqueKey(io.grpc.UniKeyQueReq request,
        io.grpc.stub.StreamObserver<io.grpc.UniKeyQueResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getQueryUniqueKeyMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class UniKeyQueSrvBlockingStub extends io.grpc.stub.AbstractStub<UniKeyQueSrvBlockingStub> {
    private UniKeyQueSrvBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private UniKeyQueSrvBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected UniKeyQueSrvBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new UniKeyQueSrvBlockingStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.UniKeyQueResp queryUniqueKey(io.grpc.UniKeyQueReq request) {
      return blockingUnaryCall(
          getChannel(), getQueryUniqueKeyMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class UniKeyQueSrvFutureStub extends io.grpc.stub.AbstractStub<UniKeyQueSrvFutureStub> {
    private UniKeyQueSrvFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private UniKeyQueSrvFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected UniKeyQueSrvFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new UniKeyQueSrvFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.grpc.UniKeyQueResp> queryUniqueKey(
        io.grpc.UniKeyQueReq request) {
      return futureUnaryCall(
          getChannel().newCall(getQueryUniqueKeyMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_QUERY_UNIQUE_KEY = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final UniKeyQueSrvImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(UniKeyQueSrvImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_QUERY_UNIQUE_KEY:
          serviceImpl.queryUniqueKey((io.grpc.UniKeyQueReq) request,
              (io.grpc.stub.StreamObserver<io.grpc.UniKeyQueResp>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class UniKeyQueSrvBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    UniKeyQueSrvBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.grpc.UniKeyQue.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("UniKeyQueSrv");
    }
  }

  private static final class UniKeyQueSrvFileDescriptorSupplier
      extends UniKeyQueSrvBaseDescriptorSupplier {
    UniKeyQueSrvFileDescriptorSupplier() {}
  }

  private static final class UniKeyQueSrvMethodDescriptorSupplier
      extends UniKeyQueSrvBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    UniKeyQueSrvMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (UniKeyQueSrvGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new UniKeyQueSrvFileDescriptorSupplier())
              .addMethod(getQueryUniqueKeyMethod())
              .build();
        }
      }
    }
    return result;
  }
}
