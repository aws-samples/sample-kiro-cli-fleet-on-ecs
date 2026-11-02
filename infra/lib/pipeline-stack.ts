/**
 * The deployment path: a commit becomes a running task definition.
 *
 * This stack is the one piece of the fleet that is not deployed by the fleet. It is brought up
 * once by hand, against a connection an operator has already authorised, and from then on a push
 * to the tracked branch is what changes the fleet. Three things about it are deliberate and would
 * otherwise have to be discovered by deploying.
 *
 * **Deploying is not running.** The last stage registers a task definition and leaves the service
 * at zero; it does not start a batch. Work begins only when the queue is seeded (`fleetctl run`),
 * which is a separate entry point with a separate blast radius. A pipeline that both deployed code
 * and launched a batch would couple a routine image change to spending a pool's credits.
 *
 * **The pipeline pushes to a repository it does not own.** The worker's ECR repository is created
 * by ComputeStack, and this stack refers to it by its deterministic name rather than by a
 * CloudFormation reference — because a reference would make the pipeline depend on the stack it is
 * meant to deploy. The consequence is a documented bootstrap: the fleet is deployed once by hand
 * (which creates the repository empty, with the service at zero), and the pipeline fills it on the
 * first build. The ComputeStack class comment is the other half of this contract.
 *
 * **The deploy stage never deploys this stack.** It is handed the names of the application stacks
 * explicitly and deploys only those. Self-mutation is a feature of CDK Pipelines that this sample
 * does without on purpose: a hand-rolled three-stage pipeline is easier to read than a construct
 * that rewrites itself, and a change to the pipeline here is a deliberate re-deploy rather than a
 * side effect of a commit.
 */

import { ArnFormat, CfnOutput, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as codebuild from 'aws-cdk-lib/aws-codebuild';
import * as codepipeline from 'aws-cdk-lib/aws-codepipeline';
import * as actions from 'aws-cdk-lib/aws-codepipeline-actions';
import * as iam from 'aws-cdk-lib/aws-iam';

export interface PipelineStackProps extends StackProps {
  /**
   * The CodeConnections (formerly CodeStar) connection to the source host.
   *
   * Created and authorised out of band, in the console, because the handshake is interactive and
   * belongs to a person rather than to a deployment. The host is whatever the connection points
   * at — GitHub for the published sample, and the same shape works for a self-managed GitLab — so
   * nothing here names a provider.
   */
  readonly connectionArn: string;

  /** The source repository as `owner/name`, matching the connection's host. */
  readonly repo: string;

  /** The branch a push to which runs the pipeline. */
  readonly branch: string;

  /**
   * Where the build instructions live in the source, relative to its root.
   *
   * The build itself is the repository's `buildspec.yml`, not a copy inlined here, so the gates a
   * pipeline runs are the same ones `tools/ci.sh` runs on a laptop. A check that exists only in the
   * pipeline is a check nobody runs before pushing.
   */
  readonly buildSpecPath: string;

  /**
   * The target architecture, which has to match what the worker image is built for.
   *
   * It decides the build container: an arm64 image must be built on arm64 compute, not under
   * emulation, because an emulated build changes every timing the image is later used to measure.
   */
  readonly architectureName: 'arm64' | 'x86_64';

  /**
   * The compute stack's name, from which the worker repository's name is derived.
   *
   * Passed as a string rather than a reference for the reason in the class comment: the pipeline
   * must not depend on the stack it deploys.
   */
  readonly computeStackName: string;

  /**
   * The application stacks the deploy stage deploys, in dependency order.
   *
   * This stack's own name is deliberately absent. CDK orders the rest from their dependencies, so
   * the list only has to be complete, not sorted.
   */
  readonly appStackNames: string[];
}

export class Pipeline extends Stack {
  readonly pipeline: codepipeline.Pipeline;

  constructor(scope: Construct, id: string, props: PipelineStackProps) {
    super(scope, id, props);

    const [owner, ...rest] = props.repo.split('/');
    const repositoryName = rest.join('/');
    if (!owner || !repositoryName) {
      throw new Error(
        `pipeline repo is ${JSON.stringify(props.repo)}; expected "owner/name" matching the ` +
          'connection host.',
      );
    }

    // The worker repository's name, derived the same way ComputeStack derives it. The two must
    // agree because the build pushes by name, so this is pinned by a test rather than trusted.
    const workerRepositoryName = `${props.computeStackName.toLowerCase()}-worker`;
    const workerRepositoryArn = this.formatArn({
      service: 'ecr',
      resource: 'repository',
      resourceName: workerRepositoryName,
      arnFormat: ArnFormat.SLASH_RESOURCE_NAME,
    });

    const source = new codepipeline.Artifact('Source');

    const sourceAction = new actions.CodeStarConnectionsSourceAction({
      actionName: 'Source',
      connectionArn: props.connectionArn,
      owner,
      repo: repositoryName,
      branch: props.branch,
      output: source,
      // A push to the branch runs the pipeline; nothing polls.
      triggerOnPush: true,
    });

    const buildAction = new actions.CodeBuildAction({
      actionName: 'Build',
      project: this.buildProject(props, workerRepositoryName, workerRepositoryArn),
      input: source,
    });

    const deployAction = new actions.CodeBuildAction({
      actionName: 'Deploy',
      project: this.deployProject(props),
      input: source,
      environmentVariables: {
        // The commit the source resolved to, so the task definition names the image built from
        // this exact revision rather than wherever `latest` points by the time a task starts.
        COMMIT_ID: { value: sourceAction.variables.commitId },
      },
    });

    this.pipeline = new codepipeline.Pipeline(this, 'Pipeline', {
      // The artifact bucket is created for us; left to CDK so it carries the encryption and
      // public-access defaults rather than a hand-written bucket that might not.
      stages: [
        { stageName: 'Source', actions: [sourceAction] },
        { stageName: 'Build', actions: [buildAction] },
        { stageName: 'Deploy', actions: [deployAction] },
      ],
    });

    new CfnOutput(this, 'PipelineName', { value: this.pipeline.pipelineName });
    new CfnOutput(this, 'WorkerRepositoryName', {
      value: workerRepositoryName,
      description: 'The ECR repository the build pushes to; created by the compute stack',
    });
  }

  /**
   * Builds the worker image on compute of the target architecture, runs the repository's gates,
   * and pushes to ECR. The instructions are the source's `buildspec.yml`; only the environment and
   * the permissions are set here.
   */
  private buildProject(
    props: PipelineStackProps,
    workerRepositoryName: string,
    workerRepositoryArn: string,
  ): codebuild.PipelineProject {
    const project = new codebuild.PipelineProject(this, 'Build', {
      buildSpec: codebuild.BuildSpec.fromSourceFilename(props.buildSpecPath),
      environment: {
        // The image must be built natively. Image id as a string rather than an enum so the
        // choice is legible and does not shift with a library upgrade.
        buildImage:
          props.architectureName === 'arm64'
            ? codebuild.LinuxArmBuildImage.fromCodeBuildImageId(
                'aws/codebuild/amazonlinux-aarch64-standard:3.0',
              )
            : codebuild.LinuxBuildImage.fromCodeBuildImageId(
                'aws/codebuild/amazonlinux-x86_64-standard:5.0',
              ),
        // Docker builds the image and runs DynamoDB Local for the contention tests.
        privileged: true,
        computeType: codebuild.ComputeType.LARGE,
      },
      environmentVariables: {
        AWS_ACCOUNT_ID: { value: this.account },
        IMAGE_REPO_NAME: { value: workerRepositoryName },
        // What the buildspec defaults to, set explicitly so the build container and the tag agree.
        TARGET_ARCH: { value: props.architectureName === 'arm64' ? 'aarch64' : 'x86_64' },
      },
    });

    // Authenticate to the registry, then push layers to this one repository. The auth token has no
    // resource to name and is granted on `*` by necessity; everything that follows is scoped to the
    // worker repository, so the test that forbids `*` on `*` still holds.
    project.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['ecr:GetAuthorizationToken'],
        resources: ['*'],
      }),
    );
    project.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          'ecr:BatchCheckLayerAvailability',
          'ecr:GetDownloadUrlForLayer',
          'ecr:BatchGetImage',
          'ecr:InitiateLayerUpload',
          'ecr:UploadLayerPart',
          'ecr:CompleteLayerUpload',
          'ecr:PutImage',
        ],
        resources: [workerRepositoryArn],
      }),
    );

    return project;
  }

  /**
   * Deploys the application stacks with `cdk deploy`, pinning the image to the built commit.
   *
   * The instructions are inlined here rather than taken from the source, because this step is not
   * part of the repository's own gates — it is how the pipeline hands the synthesized stacks to
   * CloudFormation, and it names stacks the pipeline knows and the repository does not.
   */
  private deployProject(props: PipelineStackProps): codebuild.PipelineProject {
    const project = new codebuild.PipelineProject(this, 'Deploy', {
      buildSpec: codebuild.BuildSpec.fromObject({
        version: '0.2',
        phases: {
          install: {
            'runtime-versions': { nodejs: 22 },
            commands: ['cd infra', 'npm ci'],
          },
          build: {
            commands: [
              'cd infra',
              // Only the application stacks, by name, and never this pipeline. The image tag is the
              // commit the build resolved to, so the task definition is traceable to its source.
              `npx cdk deploy ${props.appStackNames.join(' ')} ` +
                '--require-approval never --context fleet:imageTag=$COMMIT_ID',
            ],
          },
        },
      }),
      environment: {
        // No image build here, so the default x86 standard image is enough and privileged mode is
        // not needed.
        buildImage: codebuild.LinuxBuildImage.fromCodeBuildImageId(
          'aws/codebuild/amazonlinux-x86_64-standard:5.0',
        ),
        computeType: codebuild.ComputeType.SMALL,
      },
    });

    // `cdk deploy` works by assuming the roles the bootstrap created; it does not itself hold the
    // permission to change infrastructure. Scoped to this account's bootstrap roles rather than
    // granted broadly — the deploy role is where the actual deployment permission lives, and the
    // bootstrap decides what that is.
    project.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['sts:AssumeRole'],
        resources: [`arn:${this.partition}:iam::${this.account}:role/cdk-*`],
      }),
    );
    // The CLI reads the bootstrap version from SSM before it deploys, to refuse an environment that
    // was bootstrapped too long ago to be compatible.
    project.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['ssm:GetParameter'],
        resources: [
          this.formatArn({
            service: 'ssm',
            resource: 'parameter',
            resourceName: 'cdk-bootstrap/*',
            arnFormat: ArnFormat.SLASH_RESOURCE_NAME,
          }),
        ],
      }),
    );

    return project;
  }
}
