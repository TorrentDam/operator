import cats.effect.direct.*
import cats.effect.IO
import cats.effect.IOApp
import cats.syntax.all.given
import cats.Eq
import com.sun.tools.javac.util.Assert.error
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
import org.http4s.circe.*
import org.http4s.ember.client.EmberClientBuilder

import scala.concurrent.duration.Duration

object OperatorApp extends IOApp.Simple:

  def run: IO[Unit] =
    val emberConfig = EmberClientBuilder.default[IO].withIdleConnectionTime(Duration.Inf)
    val client = EmberKubernetesClient[IO](emberConfig).defaultConfig[Json]
    client.use(operatorLogic)

  def operatorLogic(client: KClient[IO]): IO[Unit] = async[IO]:
    registerCustomResource(client).await
    val operator = Operator(client)
    val namespace = sys.env.get("WATCH_NAMESPACE")
    val torrentEvents = namespace match
      case Some(ns) => new TorrentAPI(ns).list().listen(client)
      case None    => TorrentClusterAPI.list().listen(client)
    val podEvents = namespace match
      case Some(ns) => PodAPI(ns).list().listen(client)
      case None    => ClusterPodAPI.list().listen(client)
    val ops = OperatorTorrentOps(client, namespace.getOrElse("default"))
    val httpServer = TransmissionServer.stream(ops, 9091)
    torrentEvents
      .evalTap:
        case WatchEvent(WatchEventType.ADDED | WatchEventType.MODIFIED, torrent) =>
          operator.reconcile(torrent)
        case WatchEvent(WatchEventType.DELETED, torrent) =>
          operator.delete(torrent)
        case _ => IO.unit
      .merge(podEvents.evalTap(operator.onPodEvent))
      .merge(httpServer)
      .compile
      .drain
      .await

  def registerCustomResource(client: KClient[IO]): IO[Unit] = async[IO]:
    import dev.hnaderi.k8s.manifest.yamlReader
    try
      CustomResourceAPI.get("torrents.torrentdam.github.com").send(client).await
      IO.println("CRD exists").await
    catch
      case ErrorResponse(error = ErrorStatus.NotFound) =>
        val crdString = Files[IO].readUtf8(Path("crd.yaml")).compile.foldMonoid.await
        val crdYaml = IO.fromEither(SnakeYaml.parse[YAML](crdString)).await
        val crd = IO.fromEither(crdYaml.decodeTo[CustomResourceDefinition].left.map(msg => Throwable(msg))).await
        CustomResourceAPI.create(crd).send(client).await
        IO.println("CRD created").await

end OperatorApp

case class TorrentSpec(
  infoHash: String,
  pvcName: String,
  dhtNode: String,
  name: String,
  downloadPath: Option[String] = None
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
        TorrentSpec(infoHash, pvcName, dhtNode, name, downloadPath)
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

class Operator(client: KClient[IO]):

  def reconcile(resource: Torrent): IO[Unit] = async[IO]:
    val namespace = resource.metadata.namespace.getOrElse("default")
    val name = resource.metadata.name.getOrElse("")
    val podAPI = PodAPI(namespace)
    val desired = getPod(resource)
    val currentPods = podAPI.list().send(client).await
    val currentPod = currentPods.items.find(pod => pod.metadata.exists(_.name == Some(name)))
    currentPod match
      case Some(current) =>
        podAPI.replace(name, desired)
        IO.println(s"Replaced").await
      case None =>
        podAPI.create(desired).send(client).await
        IO.println("Created").await
    updateStatusByName(namespace, name).await

  def delete(resource: Torrent): IO[Unit] = async[IO]:
    val namespace = resource.metadata.namespace.getOrElse("default")
    val podAPI = PodAPI(namespace)
    val pod = getPod(resource)
    for
      metadata <- pod.metadata
      name <- metadata.name
    do
      podAPI.delete(name).send(client).void.await
      IO.println("Deleted").await

  def onPodEvent(event: WatchEvent[Pod]): IO[Unit] = event match
    case WatchEvent(WatchEventType.ADDED | WatchEventType.MODIFIED, pod) =>
      val isTorrentPod = pod.metadata.exists(_.labels.exists(_.get("app").contains("torrentdam")))
      if isTorrentPod then
        pod.metadata.flatMap(_.namespace).zip(pod.metadata.flatMap(_.name)) match
          case Some((ns, name)) => updateStatusByName(ns, name).void
          case None             => IO.unit
      else IO.unit
    case _ => IO.unit

  private def updateStatusByName(namespace: String, name: String): IO[Unit] =
    async[IO]:
      val podAPI = PodAPI(namespace)
      val torrentAPI = TorrentAPI(namespace)
      val phase =
        try
          val pod = podAPI.get(name).send(client).await
          pod.status.flatMap(_.phase).getOrElse("Unknown")
        catch
          case ErrorResponse(error = ErrorStatus.NotFound) => "Unknown"
      val torrentStatus = TorrentStatus(phase = phase, podName = name.some)
      try
        val current = torrentAPI.get(name).send(client).await
        torrentAPI.replaceStatus(name, current.copy(status = torrentStatus.some)).send(client).void.await
        IO.println(s"Status updated: $phase").await
      catch
        case ErrorResponse(error = ErrorStatus.NotFound) =>
          IO.println(s"Torrent not found, skipping status update: $name").await

  private def getPod(resource: Torrent): Pod =
    Pod(
      metadata = ObjectMeta(
        name = resource.metadata.name,
        namespace = resource.metadata.namespace,
        labels = Map("app" -> "torrentdam").some
      ).some,
      spec = PodSpec(
        restartPolicy = "OnFailure".some,
        containers = Seq(
          Container(
            name = "torrentdam",
            image = "ghcr.io/torrentdam/cmd:latest".some,
            args = Seq(
              "torrent",
              "download",
              "--info-hash",
              resource.spec.infoHash,
              "--dht-node",
              resource.spec.dhtNode
            ).some,
            workingDir = (Path("/data") / resource.spec.downloadPath.getOrElse("")).toString.some,
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
              )
            ).some,
            resources = ResourceRequirements(
              requests = Map(
                "cpu" -> Quantity("500m"),
                "memory" -> Quantity("1Gi")
              )
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

end Operator

object OperatorTorrentOps:
  def apply(client: KClient[IO], namespace: String): TorrentOps[IO] =
    new TorrentOps[IO]:
      def list: IO[List[TorrentInfo]] = async[IO]:
        val torrents = new TorrentAPI(namespace).list().send(client).await
        torrents.items.toList.map { t =>
          TorrentInfo(
            name = t.spec.name,
            infoHash = t.spec.infoHash,
            phase = t.status.flatMap(_.phase).getOrElse("Unknown"),
            downloadPath = t.spec.downloadPath
          )
        }

      def create(infoHash: String, name: String, downloadPath: Option[String]): IO[Unit] = async[IO]:
        val pvcName = sys.env.getOrElse("PVC_NAME", "movies")
        val dhtNode = sys.env.getOrElse("DHT_NODE", "server.dht.svc.cluster.local:6881")
        val crName = infoHash.toLowerCase
        val torrent = Torrent(
          spec = TorrentSpec(
            infoHash = infoHash,
            pvcName = pvcName,
            dhtNode = dhtNode,
            name = name,
            downloadPath = downloadPath
          ),
          metadata = ObjectMeta(
            name = crName.some,
            namespace = namespace.some
          )
        )
        try
          new TorrentAPI(namespace).create(torrent).send(client).await
          IO.println(s"Torrent created via API: $crName").await
        catch
          case ErrorResponse(error = ErrorStatus.Conflict) =>
            IO.println(s"Torrent already exists: $crName").await

      def delete(infoHash: String): IO[Unit] = async[IO]:
        val name = infoHash.toLowerCase
        try
          new TorrentAPI(namespace).delete(name).send(client).void.await
          IO.println(s"Torrent deleted via API: $name").await
        catch
          case ErrorResponse(error = ErrorStatus.NotFound) =>
            IO.println(s"Torrent not found for deletion: $name").await
