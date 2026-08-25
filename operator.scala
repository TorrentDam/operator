import cats.effect.direct.*
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Resource
import cats.syntax.all.given
import cats.Eq
import dev.hnaderi.k8s.circe.*
import dev.hnaderi.k8s.client.apis.corev1.{ClusterPodAPI, PodAPI}
import dev.hnaderi.k8s.client.apis.api_extensions.CustomResourceAPI
import dev.hnaderi.k8s.client.http4s.EmberKubernetesClient
import dev.hnaderi.k8s.client.http4s.KClient
import dev.hnaderi.k8s.client.{APIGroupAPI, ErrorResponse, ErrorStatus, HttpClient, WatchEvent, WatchEventType}
import dev.hnaderi.k8s.implicits.convertToOption
import dev.hnaderi.k8s.utils.*
import dev.hnaderi.yaml4s.SnakeYaml
import dev.hnaderi.yaml4s.YAML
import fs2.Stream
import fs2.io.file.{Files, Path}
import io.circe.Json
import io.k8s.api.core.v1.Container
import io.k8s.api.core.v1.ContainerPort
import io.k8s.api.core.v1.EmptyDirVolumeSource
import io.k8s.api.core.v1.EnvVar
import io.k8s.api.core.v1.PersistentVolumeClaimVolumeSource
import io.k8s.api.core.v1.Pod
import io.k8s.api.core.v1.PodSpec
import io.k8s.api.core.v1.ResourceRequirements
import io.k8s.api.core.v1.Volume
import io.k8s.api.core.v1.VolumeMount
import io.k8s.apiextensions_apiserver.pkg.apis.apiextensions.v1.CustomResourceDefinition
import io.k8s.apimachinery.pkg.api.resource.Quantity
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import io.k8s.apimachinery.pkg.apis.meta.v1.OwnerReference
import org.http4s.circe.*
import org.http4s.ember.client.EmberClientBuilder
import org.legogroup.woof.{given, *}

import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

object OperatorApp extends IOApp.Simple:

  given Filter = Filter.atLeastLevel(LogLevel.Debug)
  given Printer = NoColorPrinter()

  def run: IO[Unit] =
    val emberConfig = EmberClientBuilder.default[IO].withIdleConnectionTime(Duration.Inf)
    val client = EmberKubernetesClient[IO](emberConfig).defaultConfig[Json]
    client.use { k8sClient =>
      DefaultLogger.makeIo(Output.fromConsole[IO]).flatMap { logger =>
        operatorLogic(k8sClient, logger)
      }
    }

  def operatorLogic(client: KClient[IO], logger: Logger[IO]): IO[Unit] =
    given Logger[IO] = logger
    async[IO]:
      registerCustomResource(client).await
      val operator = Operator(client)
      val namespace = sys.env.get("WATCH_NAMESPACE")
      val torrentEvents = watchStream:
        namespace match
          case Some(ns) => new TorrentAPI(ns).list().listen(client)
          case None    => TorrentClusterAPI.list().listen(client)
      val podEvents = watchStream:
        namespace match
          case Some(ns) => PodAPI(ns).list().listen(client)
          case None    => ClusterPodAPI.list().listen(client)
      val ops = OperatorTorrentOps(client, namespace.getOrElse("default"))
      val httpServer = TransmissionServer.stream(ops, 9091)
      torrentEvents
        .evalTap:
          case WatchEvent(WatchEventType.ADDED | WatchEventType.MODIFIED, torrent) =>
            if torrent.metadata.deletionTimestamp.isDefined then
              operator.onTorrentDeletion(torrent)
            else
              operator.reconcile(torrent)
          case _ => IO.unit
        .merge(podEvents.evalTap(operator.onPodEvent))
        .merge(httpServer)
        .compile
        .drain
        .await

  def watchStream[A](stream: => Stream[IO, A])(using logger: Logger[IO]): Stream[IO, A] =
    Stream.eval(Logger[IO].info("Starting watch stream")).flatMap(_ => stream) ++
      Stream
        .eval(Logger[IO].warn("Watch stream ended, reconnecting in 5 seconds") >> IO.sleep(5.seconds))
        .flatMap(_ =>
          Stream.eval(Logger[IO].info("Reconnecting to watch stream"))
            .flatMap(_ => stream.attempt)
            .evalMap:
              case Right(a) => IO.pure(a.some)
              case Left(e)  => Logger[IO].error(s"Watch stream error: ${e.getMessage}").as(Option.empty[A])
            .unNone
        )
        .repeat

  def registerCustomResource(client: KClient[IO])(using logger: Logger[IO]): IO[Unit] = async[IO]:
    import dev.hnaderi.k8s.manifest.yamlReader
    try
      CustomResourceAPI.get("torrents.torrentdam.github.com").send(client).await
      Logger[IO].info("CRD exists").await
    catch
      case ErrorResponse(error = ErrorStatus.NotFound) =>
        val crdString = Files[IO].readUtf8(Path("crd.yaml")).compile.foldMonoid.await
        val crdYaml = IO.fromEither(SnakeYaml.parse[YAML](crdString)).await
        val crd = IO.fromEither(crdYaml.decodeTo[CustomResourceDefinition].left.map(msg => Throwable(msg))).await
        CustomResourceAPI.create(crd).send(client).await
        Logger[IO].info("CRD created").await
      case e =>
        Logger[IO].error(s"Failed to register CRD: ${e.getMessage}").await
        throw e

end OperatorApp

case class TorrentSpec(
  infoHash: String,
  pvcName: String,
  dhtNode: String,
  name: String,
  downloadPath: Option[String] = None,
  labels: List[String] = Nil
)

object TorrentSpec {
  given Encoder[TorrentSpec] = new Encoder[TorrentSpec] {
    def apply[T: Builder](o: TorrentSpec): T =
      val obj = ObjectWriter[T]()
      obj
        .write("infoHash", o.infoHash)
        .write("pvcName", o.pvcName)
        .write("dhtNode", o.dhtNode)
        .write("name", o.name)
        .write("downloadPath", o.downloadPath)
        .write("labels", o.labels)
        .build
  }

  given Decoder[TorrentSpec] = new Decoder[TorrentSpec] {
    def apply[T: Reader](t: T): Either[String, TorrentSpec] =
      for
        obj <- ObjectReader(t)
        infoHash <- obj.read[String]("infoHash")
        pvcName <- obj.read[String]("pvcName")
        dhtNode <- obj.read[String]("dhtNode")
        name <- obj.read[String]("name")
      yield
        val downloadPath = obj.read[String]("downloadPath").toOption
        val labels = obj.read[Seq[String]]("labels").toOption.getOrElse(Nil).toList
        TorrentSpec(infoHash, pvcName, dhtNode, name, downloadPath, labels)
  }
}

case class TorrentStatus(
  phase: String,
  podName: Option[String] = None
)

object TorrentStatus {
  given Encoder[TorrentStatus] = new Encoder[TorrentStatus] {
    def apply[T: Builder](o: TorrentStatus): T =
      val obj = ObjectWriter[T]()
      obj
        .write("phase", o.phase)
        .write("podName", o.podName)
        .build
  }

  given Decoder[TorrentStatus] = new Decoder[TorrentStatus] {
    def apply[T: Reader](t: T): Either[String, TorrentStatus] =
      for
        obj <- ObjectReader(t)
        phase <- obj.read[String]("phase")
      yield
        val podName = obj.read[String]("podName").toOption
        TorrentStatus(phase, podName)
  }
}

case class Torrent(
  spec: TorrentSpec,
  metadata: ObjectMeta,
  status: Option[TorrentStatus] = None
)

object Torrent {
  given Eq[Torrent] = Eq.fromUniversalEquals

  given Encoder[Torrent] = new Encoder[Torrent] {
    def apply[T: Builder](o: Torrent): T =
      val obj = ObjectWriter[T]()
      obj
        .write("kind", "Torrent")
        .write("apiVersion", "torrentdam.github.com/v1")
        .write("spec", o.spec)
        .write("metadata", o.metadata)
        .write("status", o.status)
        .build
  }

  given Decoder[Torrent] = new Decoder[Torrent] {
    def apply[T: Reader](t: T): Either[String, Torrent] =
      for
        obj <- ObjectReader(t)
        spec <- obj.read[TorrentSpec]("spec")
        metadata <- obj.read[ObjectMeta]("metadata")
      yield
        val status = obj.read[TorrentStatus]("status").toOption
        Torrent(spec, metadata, status)
  }
}

case class TorrentList(
  items: Seq[Torrent]
)

object TorrentList {
  given Decoder[TorrentList] = new Decoder[TorrentList] {
    def apply[T: Reader](t: T): Either[String, TorrentList] =
      for
        obj <- ObjectReader(t)
        items <- obj.read[Seq[Torrent]]("items")
      yield TorrentList(items)
  }
}

object TorrentAPIGroup extends APIGroupAPI("/apis/torrentdam.github.com/v1")

object TorrentAPI
    extends TorrentAPIGroup.NamespacedResourceAPI[
      Torrent,
      TorrentList
    ]("torrents")

class TorrentAPI(val namespace: String = "default") extends TorrentAPI.NamespacedAPIBuilders
object TorrentClusterAPI extends TorrentAPI.ClusterwideAPIBuilders

class Operator(client: KClient[IO])(using logger: Logger[IO]):

  private val finalizerName = "torrentdam.github.com/pvc-cleanup"

  def reconcile(resource: Torrent): IO[Unit] = async[IO]:
    val namespace = resource.metadata.namespace.getOrElse("default")
    val crName = resource.metadata.name.getOrElse("")
    val podName = podNameFor(resource)
    val podAPI = PodAPI(namespace)
    val desired = getPod(resource)
    val currentPods = podAPI.list().send(client).await
    val currentPod = currentPods.items.find(pod => pod.metadata.exists(_.name == Some(podName)))
    currentPod match
      case Some(_) =>
        Logger[IO].debug(s"Pod $podName already exists for torrent $crName").await
      case None =>
        podAPI.create(desired).send(client).await
        Logger[IO].info(s"Created pod $podName for torrent $crName").await
    ensureFinalizer(resource).await

  def onTorrentDeletion(resource: Torrent): IO[Unit] = async[IO]:
    val namespace = resource.metadata.namespace.getOrElse("default")
    val crName = resource.metadata.name.getOrElse("")
    resource.spec.downloadPath match
      case Some(downloadPath) if downloadPath.nonEmpty =>
        Logger[IO].info(s"Cleaning up PVC path /data/$downloadPath for torrent $crName").await
        waitForTorrentPodTermination(resource, namespace).await
        runCleanupPod(resource, namespace, downloadPath).await
        removeFinalizer(resource).await
        Logger[IO].info(s"Cleanup complete, finalizer removed for torrent $crName").await
      case _ =>
        removeFinalizer(resource).await

  private def waitForTorrentPodTermination(resource: Torrent, namespace: String): IO[Unit] =
    async[IO]:
      val podName = podNameFor(resource)
      val podAPI = PodAPI(namespace)
      def attempt(n: Int): IO[Unit] =
        if n >= 120 then
          Logger[IO].warn(s"Torrent pod $podName still present after 10 min, proceeding with cleanup")
        else
          val phase =
            try
              val pod = podAPI.get(podName).send(client).await
              pod.status.flatMap(_.phase).getOrElse("Unknown")
            catch
              case ErrorResponse(error = ErrorStatus.NotFound) => "Gone"
              case e =>
                Logger[IO].error(s"Failed to get torrent pod $podName: ${e.getMessage}").await
                "Unknown"
          phase match
            case "Gone" | "Succeeded" | "Failed" => IO.unit
            case _ =>
              Logger[IO].debug(s"Waiting for torrent pod $podName to terminate (phase=$phase)").await
              IO.sleep(5.seconds) *> attempt(n + 1)
      attempt(0).await

  private def ensureFinalizer(resource: Torrent): IO[Unit] = async[IO]:
    val namespace = resource.metadata.namespace.getOrElse("default")
    val crName = resource.metadata.name.getOrElse("")
    val hasFinalizer = resource.metadata.finalizers.exists(_.contains(finalizerName))
    val needsFinalizer = resource.spec.downloadPath.exists(_.nonEmpty)
    if needsFinalizer && !hasFinalizer then
      val torrentAPI = TorrentAPI(namespace)
      val updated = resource.copy(metadata = resource.metadata.addFinalizers(finalizerName))
      torrentAPI.replace(crName, updated).send(client).void.await
      Logger[IO].info(s"Added finalizer to torrent $crName").await

  private def removeFinalizer(resource: Torrent): IO[Unit] = async[IO]:
    val namespace = resource.metadata.namespace.getOrElse("default")
    val crName = resource.metadata.name.getOrElse("")
    val torrentAPI = TorrentAPI(namespace)
    val finalizers = resource.metadata.finalizers.getOrElse(Nil).filterNot(_ == finalizerName)
    val updated = resource.copy(metadata = resource.metadata.withFinalizers(finalizers))
    torrentAPI.replace(crName, updated).send(client).void.await

  private def runCleanupPod(resource: Torrent, namespace: String, downloadPath: String): IO[Unit] =
    async[IO]:
      val podAPI = PodAPI(namespace)
      val crName = resource.metadata.name.getOrElse("")
      val cleanupPodName = s"${podNameFor(resource)}-cleanup"
      val cleanupPod = Pod(
        metadata = ObjectMeta(
          name = cleanupPodName.some,
          namespace = namespace.some,
          labels = Map("app" -> "torrentdam-cleanup", "torrent" -> crName).some,
          ownerReferences = Seq(
            OwnerReference(
              apiVersion = "torrentdam.github.com/v1",
              kind = "Torrent",
              name = crName,
              uid = resource.metadata.uid.getOrElse(""),
              controller = true.some,
              blockOwnerDeletion = true.some
            )
          ).some
        ).some,
        spec = PodSpec(
          restartPolicy = "Never".some,
          terminationGracePeriodSeconds = 60L.some,
          containers = Seq(
            Container(
              name = "cleanup",
              image = "alpine:3.21".some,
              command = Seq("sh", "-c", s"rm -rf -- \"/data/$downloadPath\"").some,
              volumeMounts = Seq(
                VolumeMount(name = "data", mountPath = "/data")
              ).some
            )
          ),
          volumes = Seq(
            Volume(
              name = "data",
              persistentVolumeClaim = PersistentVolumeClaimVolumeSource(
                claimName = resource.spec.pvcName
              ).some
            )
          ).some
        ).some
      )
      try
        podAPI.create(cleanupPod).send(client).await
        Logger[IO].info(s"Created cleanup pod $cleanupPodName").await
      catch
        case ErrorResponse(error = ErrorStatus.Conflict) =>
          Logger[IO].info(s"Cleanup pod $cleanupPodName already exists, waiting for it").await
      waitForPodCompletion(podAPI, cleanupPodName, maxAttempts = 120).await

  private def waitForPodCompletion(podAPI: => PodAPI, podName: String, maxAttempts: Int): IO[Unit] =
    async[IO]:
      def attempt(n: Int): IO[Unit] =
        if n >= maxAttempts then
          for
            _ <- Logger[IO].error(s"Cleanup pod $podName did not complete in time")
            _ <- IO.raiseError(new Exception(s"Cleanup pod $podName timed out"))
          yield ()
        else
          val phase =
            try
              val pod = podAPI.get(podName).send(client).await
              pod.status.flatMap(_.phase).getOrElse("Unknown")
            catch
              case ErrorResponse(error = ErrorStatus.NotFound) => "Succeeded"
              case e =>
                Logger[IO].error(s"Failed to get cleanup pod $podName: ${e.getMessage}").await
                "Unknown"
          phase match
            case "Succeeded" =>
              Logger[IO].info(s"Cleanup pod $podName succeeded").await
              IO.unit
            case "Failed"    => IO.raiseError(new Exception(s"Cleanup pod $podName failed"))
            case _           =>
              Logger[IO].debug(s"Waiting for cleanup pod $podName (phase=$phase, attempt=$n)").await
              IO.sleep(5.seconds) *> attempt(n + 1)
      attempt(0).await

  def onPodEvent(event: WatchEvent[Pod]): IO[Unit] = event match
    case WatchEvent(WatchEventType.ADDED | WatchEventType.MODIFIED, pod) =>
      val isTorrentPod = pod.metadata.exists(_.labels.exists(_.get("app").contains("torrentdam")))
      if isTorrentPod then
        val ns = pod.metadata.flatMap(_.namespace)
        val podName = pod.metadata.flatMap(_.name)
        val crName = pod.metadata.flatMap(_.labels).flatMap(_.get("torrent"))
        (ns, podName, crName).mapN((n, p, c) => updateStatusByName(n, p, c).void)
          .getOrElse(IO.unit)
      else IO.unit
    case _ => IO.unit

  private def updateStatusByName(namespace: String, podName: String, crName: String): IO[Unit] =
    async[IO]:
      val podAPI = PodAPI(namespace)
      val torrentAPI = TorrentAPI(namespace)
      val phase =
        try
          val pod = podAPI.get(podName).send(client).await
          pod.status.flatMap(_.phase).getOrElse("Unknown")
        catch
          case ErrorResponse(error = ErrorStatus.NotFound) => "Unknown"
          case e =>
            Logger[IO].error(s"Failed to get pod $podName: ${e.getMessage}").await
            "Unknown"
      val torrentStatus = TorrentStatus(phase = phase, podName = podName.some)
      try
        val current = torrentAPI.get(crName).send(client).await
        torrentAPI.replaceStatus(crName, current.copy(status = torrentStatus.some)).send(client).void.await
        Logger[IO].info(s"Status updated for $crName: $phase").await
      catch
        case ErrorResponse(error = ErrorStatus.NotFound) =>
          Logger[IO].warn(s"Torrent not found, skipping status update: $crName").await
        case e =>
          Logger[IO].error(s"Failed to update status for $crName: ${e.getMessage}").await
          throw e

  private def podNameFor(resource: Torrent): String =
    val prefix = resource.spec.infoHash.toLowerCase.take(10)
    s"torrentdam-torrent-$prefix"

  private def getPod(resource: Torrent): Pod =
    val downloadPath = resource.spec.downloadPath.getOrElse("")
    val baseContainer = Container(
      name = "torrentdam",
      image = "ghcr.io/torrentdam/cmd:latest".some,
      args = Seq(
        "torrent",
        "download",
        "--info-hash",
        resource.spec.infoHash,
        "--dht-node",
        resource.spec.dhtNode,
        "--events",
        "/var/torrentdam/events.json"
      ).some,
      workingDir = (Path("/data") / downloadPath).toString.some,
      env = Seq(
        EnvVar(
          name = "INFO_HASH",
          value = resource.spec.infoHash.some
        )
      ).some,
      volumeMounts = Seq(
        VolumeMount(
          name = "data",
          mountPath = "/data"
        ),
        VolumeMount(
          name = "events",
          mountPath = "/var/torrentdam"
        )
      ).some,
      resources = ResourceRequirements(
        requests = Map(
          "cpu" -> Quantity("500m"),
          "memory" -> Quantity("1Gi")
        )
      ).some
    )
    val container = baseContainer
    val eventsContainer = Container(
      name = "events",
      image = "alpine:3.21".some,
      command = Seq(
        "nc",
        "-lk",
        "-p",
        "9000",
        "-e",
        "tail",
        "-F",
        "-n",
        "0",
        "/var/torrentdam/events.json"
      ).some,
      volumeMounts = Seq(
        VolumeMount(
          name = "events",
          mountPath = "/var/torrentdam"
        )
      ).some,
      ports = Seq(
        ContainerPort(
          containerPort = 9000,
          name = "events".some
        )
      ).some,
      restartPolicy = "Always".some
    )
    Pod(
      metadata = ObjectMeta(
        name = podNameFor(resource).some,
        namespace = resource.metadata.namespace,
        labels = Map(
          "app" -> "torrentdam",
          "torrent" -> resource.metadata.name.getOrElse("")
        ).some,
        ownerReferences = Seq(
          OwnerReference(
            apiVersion = "torrentdam.github.com/v1",
            kind = "Torrent",
            name = resource.metadata.name.getOrElse(""),
            uid = resource.metadata.uid.getOrElse(""),
            controller = true.some,
            blockOwnerDeletion = true.some
          )
        ).some
      ).some,
      spec = PodSpec(
        restartPolicy = "OnFailure".some,
        terminationGracePeriodSeconds = 300L.some,
        initContainers = Seq(eventsContainer).some,
        containers = Seq(container),
        volumes = Seq(
          Volume(
            name = "data",
            persistentVolumeClaim = PersistentVolumeClaimVolumeSource(
              claimName = resource.spec.pvcName
            ).some
          ),
          Volume(
            name = "events",
            emptyDir = EmptyDirVolumeSource().some
          )
        ).some
      ).some
    )

end Operator

object OperatorTorrentOps:
  def apply(client: KClient[IO], namespace: String)(using logger: Logger[IO]): TorrentOps[IO] =
    new TorrentOps[IO]:
      def list: IO[List[TorrentInfo]] = async[IO]:
        val torrents = new TorrentAPI(namespace).list().send(client).await
        torrents.items.toList.map { t =>
          TorrentInfo(
            name = t.spec.name,
            infoHash = t.spec.infoHash,
            phase = t.status.flatMap(_.phase).getOrElse("Unknown"),
            downloadPath = t.spec.downloadPath,
            labels = t.spec.labels
          )
        }

      def create(infoHash: String, name: String, downloadPath: Option[String], labels: List[String]): IO[Unit] = async[IO]:
        val pvcName = sys.env.getOrElse("PVC_NAME", "movies")
        val dhtNode = sys.env.getOrElse("DHT_NODE", "server.dht.svc.cluster.local:6881")
        val crName = infoHash.toLowerCase
        val torrent = Torrent(
          spec = TorrentSpec(
            infoHash = infoHash,
            pvcName = pvcName,
            dhtNode = dhtNode,
            name = name,
            downloadPath = downloadPath,
            labels = labels
          ),
          metadata = ObjectMeta(
            name = crName.some,
            namespace = namespace.some
          )
        )
        try
          new TorrentAPI(namespace).create(torrent).send(client).await
          Logger[IO].info(s"Torrent created via API: $crName").await
        catch
          case ErrorResponse(error = ErrorStatus.Conflict) =>
            Logger[IO].info(s"Torrent already exists: $crName").await
          case e =>
            Logger[IO].error(s"Failed to create torrent $crName: ${e.getMessage}").await
            throw e

      def delete(infoHash: String): IO[Unit] = async[IO]:
        val name = infoHash.toLowerCase
        try
          new TorrentAPI(namespace).delete(name).send(client).void.await
          Logger[IO].info(s"Torrent deleted via API: $name").await
        catch
          case ErrorResponse(error = ErrorStatus.NotFound) =>
            Logger[IO].warn(s"Torrent not found for deletion: $name").await
          case e =>
            Logger[IO].error(s"Failed to delete torrent $name: ${e.getMessage}").await
            throw e