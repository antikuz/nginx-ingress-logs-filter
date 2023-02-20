package main

import (
	"bufio"
	"context"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	logger *zap.Logger
	wg     sync.WaitGroup

	clientset      *kubernetes.Clientset
	namespace      = os.Getenv("INGRESSNAMESPACE")
	podIngressName = os.Getenv("INGRESSPODNAME")
	logLevel       = os.Getenv("LOGLEVEL")
	searchString   = os.Getenv("SEARCHSTRING")
	follow         = true

	podsWatched       = map[string]bool{}
)

func logParser(ctx context.Context, logChannel chan string) {
	defer wg.Done()

	for {
		select {
		case log := <-logChannel:
			logger.Sugar().Debugf("Get string: %s", log)
			if strings.Contains(log, searchString) {
				logger.Info(log)
			}
		case <-ctx.Done():
			return
		}
	}
}

func watchPodLogs(ctx context.Context, podName string, containerName string, logChannel chan string) {
	defer wg.Done()
	podsWatched[podName] = true
	defer delete(podsWatched, podName)
	defer logger.Sugar().Infof("Pod deleted %s/%s, remove from watch", podName, containerName)

	count := int64(0) // only new lines read
	podLogOptions := corev1.PodLogOptions{
		Container: containerName,
		Follow:    follow,
		TailLines: &count,
	}

	for {
		timeout := time.After(5 * time.Minute)
		podLogRequest := clientset.CoreV1().Pods(namespace).GetLogs(podName, &podLogOptions)
		stream, err := podLogRequest.Stream(ctx)
		if err != nil {
			logger.Sugar().Errorf("Unable to get %s/%s log stream, due to err: %v", podName, containerName, err)
			return
		}

		go func() {
			reader := bufio.NewScanner(stream)
			for reader.Scan() {
				logChannel <- reader.Text()
			}
		}()

		select {
		case <-ctx.Done():
			logger.Sugar().Infof("Log scanner %s/%s closed due context cancel", podName, containerName)
			if err = stream.Close(); err != nil {
				logger.Sugar().Errorf("Log scanner %s/%s get error while podLogRequest.Stream close: %v", podName, containerName, err)
			}
			return
		case <-timeout:
			break
		}

		if err = stream.Close(); err != nil {
			logger.Sugar().Errorf("Log scanner %s/%s get error while podLogRequest.Stream close: %v", podName, containerName, err)
		}
	}
}

func isPodLogWatched(podName string) bool {
	_, ok := podsWatched[podName]
	return ok
}

func podIsReady(podConditions []corev1.PodCondition) bool {
	for _, condition := range podConditions {
		if condition.Type == "Ready" && condition.Status == "True" {
			return true
		}
	}
	return false
}

func getPods(ctx context.Context, namespace string) (*corev1.PodList, error) {
	podInterface := clientset.CoreV1().Pods(namespace)
	podList, err := podInterface.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return podList, nil
}

func podEventProcessing(ctx context.Context, event watch.Event, pod *corev1.Pod, logChannel chan string) {
	if !strings.Contains(pod.Name, podIngressName) || isPodLogWatched(pod.Name) {
		return
	}

	switch event.Type {
	case watch.Modified:
		switch pod.Status.Phase {
		case corev1.PodRunning:
			if podIsReady(pod.Status.Conditions) {
				logger.Sugar().Infof("Found new pod created: %s, add to watching logs\n", pod.Name)
				go watchPodLogs(ctx, pod.Name, pod.Spec.Containers[0].Name, logChannel)
			}
		}
	}
}

func watchEventListener(ctx context.Context, watcher watch.Interface, logChannel chan string) error {
	for {
		select {
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return nil
			}

			if pod, ok := event.Object.(*corev1.Pod); ok {
				podEventProcessing(ctx, event, pod, logChannel)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// watch for new created pods and add to logging
func watchPods(ctx context.Context, logChannel chan string) {
	defer wg.Done()

	for {
		watcher, err := clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{})
		if err != nil {
			logger.Sugar().Fatalf("cannot create Pod event watcher, due to err: %v", err)
		}

		if err = watchEventListener(ctx, watcher, logChannel); err != nil {
			watcher.Stop()
			return
		}
	}
}

func main() {
	if logLevel == "debug" {
		logger = zap.Must(zap.NewDevelopment())
	} else {
		logger = zap.Must(zap.NewProduction())
	}

	wg = sync.WaitGroup{}

	logChannel := make(chan string)
	ctx, _ := signal.NotifyContext(
		context.Background(),
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	podList, err := getPods(ctx, namespace)
	if err != nil {
		panic(err)
	}

	logger.Sugar().Infof("Search pods in namespace: \"%s\" and name should contains: \"%s\"", namespace, podIngressName)
	for _, pod := range podList.Items {
		if strings.Contains(pod.Name, podIngressName) {
			logger.Sugar().Infof("Found pod: \"%s\", add to watching logs", pod.Name)
			wg.Add(1)
			go watchPodLogs(ctx, pod.Name, pod.Spec.Containers[0].Name, logChannel)
		}
	}

	wg.Add(3)
	go watchPods(ctx, logChannel)
	go logParser(ctx, logChannel)
	go logParser(ctx, logChannel)

	wg.Wait()
}
