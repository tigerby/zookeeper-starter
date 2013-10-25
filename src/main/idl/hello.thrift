namespace java com.tigerby.thrift.generated

service HelloService {
    string greeting(1:string name, 2:i32 age)
}