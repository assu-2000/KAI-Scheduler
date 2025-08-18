# Lazy string evaluations
Compare these two code snippets:
1: 
```go
		log.InfraLogger.V(7).Infof("Proportion DeallocateFunc: job <%v/%v>, task resources <%v>, queue: <%v>, queue allocated resources: <%v>",
			job.Namespace, job.Name, taskResources.String(), leafQueue.Name, leafQueue.GetAllocatedShare())
```

2: 
```go
		log.InfraLogger.V(7).Infof("Proportion DeallocateFunc: job <%v/%v>, task resources <%s>, queue: <%v>, queue allocated resources: <%v>",
			job.Namespace, job.Name, taskResources, leafQueue.Name, leafQueue.GetAllocatedShare())
```

They look almost identical. What’s the difference?
![](Lazy%20string%20evaluations/image.png)

Let’s dive into what happens when we call `InfraLogger.V()`:

```Go
func (sl *schedulerLogger) V(lvl int) *zap.SugaredLogger {
    if sl.logLevel >= lvl {
        return sl.getLogger()
    }
    return emptyLogger
}
```

Now, what happens if the log level is too low?
We go into:

```Go
// log message with Sprint, Sprintf, or neither.
func (s *SugaredLogger) log(lvl zapcore.Level, template string, fmtArgs []interface{}, context []interface{}) {
    // If logging at this level is completely disabled, skip the overhead of
    // string formatting.
    if lvl < DPanicLevel && !s.base.Core().Enabled(lvl) {
        return
    }

    msg := getMessage(template, fmtArgs)
    if ce := s.base.Check(lvl, msg); ce != nil {
        ce.Write(s.sweetenFields(context)...)
    }
}

```

Note that we skip the string formatting in this case! All the arguments that were passed from the function calling the logger remain as they are! For example, in our case:
![](Lazy%20string%20evaluations/image%202.png)

Specifically note the arg in index [2]:
![](Lazy%20string%20evaluations/image%203.png)

This is a ResourceQuantities struct. What was happening before we made the change to the line above, when we had the explicit call to `.String()`?

![](Lazy%20string%20evaluations/image%204.png)

Note that we now got a nice formatted string!
However, the log level is too low, meaning it will not be printed at all.. Meaning that we formatted it for nothing!

This might sound like no big deal - but not that this is the proportion  `deallocateFunc`, which is called for every evicted task in every simulation… And the string formatting for resource calls `NewResource`, creating a new object in memory:

```go
func NewResource(milliCPU float64, memory float64, gpus float64) *Resource {
    return &Resource{
        gpus:         gpus,
        BaseResource: *NewBaseResourceWithValues(milliCPU, memory),
    }
}
```

And populates it with values from it’s map:

```Go
func (rq ResourceQuantities) String() string {
    return resource_info.NewResource(
        rq[CpuResource], rq[MemoryResource], rq[GpuResource],
    ).String()
}
```

These are a lot of memory allocations and lookups for a string we won’t use! Note that the log level for this line is 7 - meaning it will only be seen on the most severe of debug sessions!

When we use the fixed version, the `.String()` function will still be called, because when a string format has `%s`, this is what go does. However, if the log level is too low, this function will not be called, because the arguments to the function will be discarded before the format will be evaluated!