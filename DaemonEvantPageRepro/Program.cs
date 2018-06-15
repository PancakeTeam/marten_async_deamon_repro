using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Baseline.Dates;
using Marten;
using Marten.Events.Projections;
using Marten.Events.Projections.Async;
using Marten.Storage;

namespace DaemonEvantPageRepro
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var marten = SetupMarten();
            //Task.Run(() => SpitEvents(marten));

            while (true)
            {
                Console.ReadKey();
                GC.Collect();
            }
        }

        private static async Task SpitEvents(IDocumentStore store)
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(5));
                using (var session = store.OpenSession())
                {
                    session.Events.Append(Guid.NewGuid(), new EventType1(), new EventType2(), new EventType3(),
                        new EventType4(), new EventType5(), new EventType6(), new EventType7(), new EventType8(),
                        new EventType9(), new EventType9(), new EventType10(), new EventType11());
                    await session.SaveChangesAsync();
                }
            }
        }

        private static IDocumentStore SetupMarten()
        {
            IDocumentStore store = DocumentStore.For(_ =>
            {
                _.Connection(
                    "host=10.0.75.2;port=5433;database=daemon;password=postgres;username=postgres;MaxPoolSize=200;ConnectionIdleLifetime=30");
                _.PLV8Enabled = false;
                _.UseDefaultSerialization(EnumStorage.AsInteger, Casing.CamelCase);
                _.DatabaseSchemaName = "public";
                _.AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate;
                _.Events.AddEventTypes(new[]
                {
                    typeof(EventType1), typeof(EventType2), typeof(EventType3), typeof(EventType4), typeof(EventType5), typeof(EventType6),
                    typeof(EventType7), typeof(EventType8), typeof(EventType9), typeof(EventType10), typeof(EventType11), typeof(EventType12)
                });
                _.Events.AsyncProjections.Add(new P1());
                _.Events.AsyncProjections.Add(new P2());
                _.Events.AsyncProjections.Add(new P3());
                _.Events.AsyncProjections.Add(new P4());
                _.Events.AsyncProjections.Add(new P5());
                _.Events.AsyncProjections.Add(new P6());
                _.Events.AsyncProjections.Add(new P7());
                _.Events.AsyncProjections.Add(new P8());
                _.Events.AsyncProjections.Add(new P9());
                _.Events.AsyncProjections.Add(new P10());
                _.Events.AsyncProjections.Add(new P11());
                _.Events.AsyncProjections.Add(new P12());
            });

            store.Schema.ApplyAllConfiguredChangesToDatabase();
            store.Schema.AssertDatabaseMatchesConfiguration();

            var daemonSetting = new DaemonSettings { LeadingEdgeBuffer = 0.Seconds() };
            daemonSetting.ExceptionHandling.OnException(e => true).Retry(3, 3.Seconds());
            daemonSetting.ExceptionHandling.Cooldown = 10.Seconds();

            var daemon = store.BuildProjectionDaemon(settings: daemonSetting);
            daemon.StartAll();

            return store;
        }
    }

    public class EventType1 { }
    public class EventType2 { }
    public class EventType3 { }
    public class EventType4 { }
    public class EventType5 { }
    public class EventType6 { }
    public class EventType7 { }
    public class EventType8 { }
    public class EventType9 { }
    public class EventType10 { }
    public class EventType11 { }
    public class EventType12 { }

    public class P1 : AsyncProjection<EventType1> { }
    public class P2 : AsyncProjection<EventType2> { }
    public class P3 : AsyncProjection<EventType3> { }
    public class P4 : AsyncProjection<EventType4> { }
    public class P5 : AsyncProjection<EventType5> { }
    public class P6 : AsyncProjection<EventType6> { }
    public class P7 : AsyncProjection<EventType7> { }
    public class P8 : AsyncProjection<EventType8> { }
    public class P9 : AsyncProjection<EventType9> { }
    public class P10 : AsyncProjection<EventType10> { }
    public class P11 : AsyncProjection<EventType11> { }
    public class P12 : AsyncProjection<EventType12> { }

    public class AsyncProjection<TEvent> : IProjection
    {
        public void Apply(IDocumentSession session, EventPage page)
        {
        }

        public Task ApplyAsync(IDocumentSession session, EventPage page, CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public void EnsureStorageExists(ITenant tenant)
        {
        }

        public Type[] Consumes => new[] { typeof(TEvent) };

        public AsyncOptions AsyncOptions => new AsyncOptions();
    }
}
