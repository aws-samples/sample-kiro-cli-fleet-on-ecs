/**
 * Where the tasks run, and what they are allowed to reach.
 *
 * Three modes, set with `fleet:egressControl`. In `private`, the default, the task subnets have no
 * route to the internet at all: every destination the workload needs has a VPC endpoint, and the
 * Kiro service is one of them. `inspection` adds a NAT gateway and a firewall allowlist for a
 * workflow that needs the internet; `open` adds a NAT gateway with no destination control.
 *
 * The destination list is measured rather than reasoned. A capture of one credentialed run
 * (hostnames and byte counts, no decryption), then a deployment in `private` mode, gave this:
 *
 *   interface endpoint, volume          image pulls, 0.69 GB per host
 *   interface endpoint, small           queue, secrets, logs, STS
 *   q interface endpoint, required      runtime.<region>.kiro.dev  59% of a run, inference
 *                                       management.<region>.kiro.dev  8.9%, control plane
 *   q interface endpoint, not required  telemetry.<region>.kiro.dev
 *   unreachable, not required           other telemetry and cognito-identity, which block cleanly
 *   never contacted                     codewhisperer, and auth.desktop under API-key auth
 *
 * Two corrections matter more than the list. The `q` endpoint provides private DNS for the three
 * `kiro.dev` names, so inference reaches the Kiro service inside the VPC and no internet path is
 * needed for it; an earlier version of this stack assumed otherwise and routed it through the
 * firewall. And `codewhisperer` was provisioned on the assumption that it carried inference and
 * was never resolved once. Ongoing traffic totals 0.39 MB per run, so the endpoints are justified
 * by the image pull and by keeping credentials and results off the public internet, not by the
 * conversation.
 *
 * When a mode does build an internet path, the tasks are *routed* through it. A firewall only
 * inspects what reaches it, and a subnet CDK considers to have egress gets a default route
 * straight to the NAT gateway. See `routeEgress`.
 */

import { Annotations, CfnOutput, Fn, RemovalPolicy, Stack, StackProps, Tags } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as networkfirewall from 'aws-cdk-lib/aws-networkfirewall';
import * as customresources from 'aws-cdk-lib/custom-resources';
import { FleetConfig, fleetConfig } from './config';

/**
 * AWS services the workload calls that offer an interface endpoint.
 *
 * `q` is here because it carries the Kiro service: it provides private DNS for
 * `runtime.<region>.kiro.dev` (inference), `management` and `telemetry`, which is what lets the
 * default mode run with no internet route. `codewhisperer` was here once and is not; see below.
 */
const INTERFACE_ENDPOINTS: Record<string, ec2.InterfaceVpcEndpointAwsService> = {
  // The pair that carries real volume: 0.69 GB of image layers per host.
  EcrApi: ec2.InterfaceVpcEndpointAwsService.ECR,
  EcrDocker: ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER,
  // These carry very little — a capture of one run measured 0.39 MB in total across every
  // destination. They are here for exposure rather than for cost: each one keeps a call that
  // carries a credential or a result off the public internet. Worth naming honestly, because
  // the earlier justification for them was a volume argument that the measurement did not
  // support.
  Sqs: ec2.InterfaceVpcEndpointAwsService.SQS,
  SecretsManager: ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
  CloudWatchLogs: ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
  Sts: ec2.InterfaceVpcEndpointAwsService.STS,
  // The Kiro service, inference included: runtime, management and telemetry under kiro.dev
  // resolve to this endpoint. Without it the default mode has no way to reach Kiro.
  AmazonQ: new ec2.InterfaceVpcEndpointAwsService('q'),
  // `codewhisperer` was here on the assumption that it carried inference. A capture of a
  // credentialed run never resolved it once, so it is gone: an endpoint nothing uses is
  // $14.60 a month for nothing. That an endpoint exists for a service is not evidence that
  // this workload calls it.
};

/**
 * Which tier a task belongs in, named once.
 *
 * Exported because the answer changed: tasks used to sit in the only private tier, and now
 * there are two and only one of them is right. A stack that picked `PRIVATE_WITH_EGRESS` by
 * name would land its tasks in the firewall's tier, where the default route goes straight to
 * the gateway — the same bypass this stack was just fixed for, reintroduced from the other side.
 */
export const TASK_SUBNETS: ec2.SubnetSelection = {
  subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
};

export class NetworkStack extends Stack {
  readonly vpc: ec2.Vpc;
  readonly taskSecurityGroup: ec2.SecurityGroup;

  /** Where anything running the workload goes. Use this rather than naming a subnet type. */
  readonly taskSubnets: ec2.SubnetSelection = TASK_SUBNETS;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);
    const config: FleetConfig = fleetConfig(this);

    // The tasks' tier, always. Isolated rather than merely private: CDK writes a default route
    // to a NAT gateway for a subnet it considers to have egress, and that route is what let the
    // firewall be bypassed when this stack had one. With no default route, whatever reaches the
    // internet has to be written down deliberately.
    const taskTier: ec2.SubnetConfiguration = {
      name: 'Tasks',
      subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
      cidrMask: 20,
    };

    // The tiers an egress path needs, and only when one is being built. In the default mode
    // there is nothing to put in them: no gateway, no firewall, no public subnet at all.
    const egressTiers: ec2.SubnetConfiguration[] =
      config.egressControl === 'private'
        ? []
        : [
            {
              name: 'Egress',
              subnetType: ec2.SubnetType.PUBLIC,
              cidrMask: 24,
              // Nothing runs here. It exists to hold the gateway.
              mapPublicIpOnLaunch: false,
            },
            {
              // Where the firewall's endpoint lives. A separate tier because traffic has to
              // reach the firewall and then leave, and one route table cannot express both: the
              // tier being inspected routes to the firewall, and the firewall's own tier routes
              // to the gateway.
              name: 'Inspection',
              subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
              cidrMask: 24,
            },
          ];

    this.vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2,
      // None in the default mode, which is the whole saving. Otherwise one rather than one per
      // zone: a sample does not need zonal independence for this path, and the second gateway is
      // a standing hourly charge.
      natGateways: config.egressControl === 'private' ? 0 : 1,
      subnetConfiguration: [...egressTiers, taskTier],
      // A record of every connection the tasks make, so the egress claims above can be checked
      // against traffic rather than taken on trust. Kept as long as the fleet's own logs.
      flowLogs: {
        All: {
          destination: ec2.FlowLogDestination.toCloudWatchLogs(
            new logs.LogGroup(this, 'VpcFlowLogs', {
              retention: config.logRetention,
              removalPolicy: RemovalPolicy.DESTROY,
            }),
          ),
          trafficType: ec2.FlowLogTrafficType.ALL,
        },
      },
    });

    this.taskSecurityGroup = this.createTaskSecurityGroup(config);
    this.addEndpoints();
    this.routeEgress(config);
    this.publishOutputs();
  }

  /**
   * The tasks' own group: outbound 443 only, nothing inbound.
   *
   * Outbound is narrowed to HTTPS rather than left at the default allow-all, which is what makes
   * the claim about egress mean something. Nothing needs to reach a task, so there are no ingress
   * rules at all.
   */
  private createTaskSecurityGroup(config: FleetConfig): ec2.SecurityGroup {
    const group = new ec2.SecurityGroup(this, 'TaskSecurityGroup', {
      vpc: this.vpc,
      description: 'Worker tasks: HTTPS out, nothing in',
      // The default rule is allow-all outbound, which would make every other control here
      // decorative.
      allowAllOutbound: false,
    });

    // Interface endpoints resolve to addresses inside this CIDR, the Kiro service included —
    // the `q` endpoint answers for `runtime`, `management` and `telemetry` under `kiro.dev`.
    group.addEgressRule(
      ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
      ec2.Port.tcp(443),
      'Interface endpoints, and the Kiro service through the q endpoint',
    );

    // Gateway endpoints are different, and the difference cost a failed deployment to find. They
    // do not change the destination address: S3 still resolves to a public address and only the
    // route changes, so a group permitting the VPC CIDR alone blocks them. The first task could
    // not pull its own image — ECR serves layers from S3 — and reported a connection timeout to
    // an address the route table was perfectly willing to carry.
    //
    // The managed prefix lists are what a group can name for them. Their identifiers differ by
    // region, so they are looked up rather than written down.
    for (const [name, list] of Object.entries(this.gatewayPrefixLists())) {
      group.addEgressRule(
        ec2.Peer.prefixList(list),
        ec2.Port.tcp(443),
        `${name} through its gateway endpoint, which keeps a public destination address`,
      );
    }

    if (config.egressControl !== 'private') {
      group.addEgressRule(
        ec2.Peer.anyIpv4(),
        ec2.Port.tcp(443),
        'Whatever the workflow needs from the internet. A security group works in addresses, ' +
          'so the destination is constrained by the firewall rather than here.',
      );
    }

    Tags.of(group).add('EgressPolicy', config.egressControl === 'private' ? 'vpc-only' : 'https-only');
    return group;
  }

  private addEndpoints(): void {
    // In the tasks' own tier. Endpoints are reached over the VPC's local route, which exists
    // whatever the default route does, so an isolated tier reaches them normally.
    const subnets = { subnetType: ec2.SubnetType.PRIVATE_ISOLATED };

    // Gateway endpoints are free and route in the route table, so there is no reason not to have
    // them. S3 carries the largest share of traffic after image layers.
    this.vpc.addGatewayEndpoint('S3', {
      service: ec2.GatewayVpcEndpointAwsService.S3,
      subnets: [subnets],
    });
    this.vpc.addGatewayEndpoint('DynamoDb', {
      service: ec2.GatewayVpcEndpointAwsService.DYNAMODB,
      subnets: [subnets],
    });

    // Interface endpoints are billed at $0.01 an hour per endpoint per zone, about $14.60 a
    // month each. The ECR pair pays for itself on volume — 0.69 GB per host against $0.045 a
    // gigabyte through the gateway. The rest do not, and are kept for exposure instead: see the
    // map above, which says which is which rather than claiming volume for all of them.
    for (const [name, service] of Object.entries(INTERFACE_ENDPOINTS)) {
      this.vpc.addInterfaceEndpoint(name, {
        service,
        subnets,
        privateDnsEnabled: true,
        securityGroups: [this.endpointSecurityGroup],
      });
    }
  }

  private _endpointSecurityGroup?: ec2.SecurityGroup;

  /** Accepts 443 from the VPC, so only things inside can use the endpoints. */
  private get endpointSecurityGroup(): ec2.SecurityGroup {
    if (!this._endpointSecurityGroup) {
      this._endpointSecurityGroup = new ec2.SecurityGroup(this, 'EndpointSecurityGroup', {
        vpc: this.vpc,
        description: 'Interface endpoints: HTTPS from within the VPC',
        allowAllOutbound: false,
      });
      this._endpointSecurityGroup.addIngressRule(
        ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
        ec2.Port.tcp(443),
        'Tasks and functions in this VPC',
      );
    }
    return this._endpointSecurityGroup;
  }

  /**
   * A domain allowlist on the one path that has to leave the VPC.
   *
   * Without this the gateway is an open door and the security claim is wording only. A firewall is
   * the straightforward option; an egress proxy is cheaper and more work, and is noted in the
   * README as the alternative for a cost-sensitive deployment.
   *
   * Stateful rules are ordered: allow the named domains, then drop everything else. The default
   * action matters as much as the rules, because a firewall that defaults to allow inspects
   * traffic and permits it anyway.
   */
  /**
   * Send the tasks' outbound traffic through the firewall, then out.
   *
   * This is the part that was missing, and its absence is the most instructive thing in this
   * stack. The firewall resource existed, the rules named the right domains, the deletion
   * protections were on, and a test asserted all of it. Traffic went from the tasks straight to
   * the NAT gateway and never touched any of it — because CDK writes that route for a
   * `PRIVATE_WITH_EGRESS` subnet, and a firewall only inspects what is routed to it.
   *
   * Everything about the configuration was right except whether it was in the path, which is
   * the one thing none of the assertions covered. The test now checks the route.
   */
  private routeEgress(config: FleetConfig): void {
    const taskSubnets = this.vpc.selectSubnets({
      subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
    }).subnets;

    if (config.egressControl === 'private') {
      // Nothing to do, and that is the point. No route is written, so there is no path to the
      // internet to constrain — the strongest form of the control and the cheapest. Verified by
      // deployment rather than assumed: every destination the workload needs resolves to an
      // address inside the VPC, the Kiro service included.
      return;
    }

    if (config.egressControl === 'open') {
      Annotations.of(this).addWarning(
        "fleet:egressControl is 'open', so egress is restricted by port but not by " +
          'destination. A task can reach any host on 443. Prefer \'private\', which needs no ' +
          'gateway at all, unless a workflow genuinely requires the internet.',
      );

      for (const [index, subnet] of taskSubnets.entries()) {
        new ec2.CfnRoute(this, `TaskDefaultRoute${index}`, {
          routeTableId: subnet.routeTable.routeTableId,
          destinationCidrBlock: '0.0.0.0/0',
          natGatewayId: this.natGatewayId(),
        });
      }
      return;
    }

    const firewall = this.createFirewall(config);

    // `attrEndpointIds` yields entries shaped "us-east-1a:vpce-0123", one per subnet the
    // firewall was placed in, so the identifier has to be split out of the pair.
    const endpointId = Fn.select(1, Fn.split(':', Fn.select(0, firewall.attrEndpointIds)));

    for (const [index, subnet] of taskSubnets.entries()) {
      const route = new ec2.CfnRoute(this, `TaskEgressViaFirewall${index}`, {
        routeTableId: subnet.routeTable.routeTableId,
        destinationCidrBlock: '0.0.0.0/0',
        vpcEndpointId: endpointId,
      });
      // The endpoint has to exist before a route can name it.
      route.addDependency(firewall);
    }
  }

  private requireEgressDomains(config: FleetConfig): void {
    if (config.kiroEgressDomains.length === 0) {
      // Refused rather than defaulted. Guessing the domains would produce a fleet that cannot
      // authenticate, and an empty allowlist would produce one that cannot either — silently.
      throw new Error(
        'fleet:kiroEgressDomains is empty. The tasks need to reach the Kiro service to ' +
          'authenticate and to run a model, so name those endpoints in cdk.json.',
      );
    }
  }

  /**
   * The managed prefix list identifiers for the gateway endpoints, by region.
   *
   * Looked up at deploy time because the identifiers are regional and undocumented per region;
   * writing `pl-63a5400a` here would make the stack deployable in one region only, and would
   * fail in another with a security group rule naming a prefix list that does not exist.
   *
   * A custom resource for two strings is not free, and the alternative — asking an operator to
   * paste the identifiers into context — moves a lookup anyone can automate into a manual step
   * that silently breaks a redeployment elsewhere.
   */
  private gatewayPrefixLists(): Record<string, string> {
    const lookup = (service: 's3' | 'dynamodb') => {
      const resource = new customresources.AwsCustomResource(
        this,
        `PrefixList${service === 's3' ? 'S3' : 'DynamoDb'}`,
        {
          onUpdate: {
            service: 'EC2',
            action: 'describeManagedPrefixLists',
            parameters: {
              Filters: [
                {
                  Name: 'prefix-list-name',
                  Values: [`com.amazonaws.${this.region}.${service}`],
                },
              ],
            },
            physicalResourceId: customresources.PhysicalResourceId.of(
              `prefix-list-${service}`,
            ),
          },
          policy: customresources.AwsCustomResourcePolicy.fromSdkCalls({
            resources: customresources.AwsCustomResourcePolicy.ANY_RESOURCE,
          }),
          installLatestAwsSdk: false,
        },
      );
      return resource.getResponseField('PrefixLists.0.PrefixListId');
    };

    return { 'Object storage': lookup('s3'), Tables: lookup('dynamodb') };
  }

  private natGatewayId(): string {
    const gateway = this.vpc.node
      .findAll()
      .find((child): child is ec2.CfnNatGateway => child instanceof ec2.CfnNatGateway);
    if (!gateway) {
      throw new Error('no NAT gateway was created, so there is no egress path to route to');
    }
    return gateway.ref;
  }

  private createFirewall(config: FleetConfig): networkfirewall.CfnFirewall {
    this.requireEgressDomains(config);

    const ruleGroup = new networkfirewall.CfnRuleGroup(this, 'EgressAllowlist', {
      capacity: 100,
      ruleGroupName: `${this.stackName}-kiro-egress`,
      type: 'STATEFUL',
      ruleGroup: {
        rulesSource: {
          rulesSourceList: {
            generatedRulesType: 'ALLOWLIST',
            targetTypes: ['TLS_SNI', 'HTTP_HOST'],
            targets: config.kiroEgressDomains,
          },
        },
      },
    });

    const policy = new networkfirewall.CfnFirewallPolicy(this, 'EgressPolicy', {
      firewallPolicyName: `${this.stackName}-egress`,
      firewallPolicy: {
        // Drop by default. An allowlist in front of a permissive default allows everything.
        statelessDefaultActions: ['aws:forward_to_sfe'],
        statelessFragmentDefaultActions: ['aws:forward_to_sfe'],
        statefulDefaultActions: ['aws:drop_strict', 'aws:alert_strict'],
        statefulEngineOptions: { ruleOrder: 'STRICT_ORDER' },
        statefulRuleGroupReferences: [{ resourceArn: ruleGroup.attrRuleGroupArn, priority: 1 }],
      },
    });

    const inspection = this.vpc.selectSubnets({
      subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
    }).subnets;

    return new networkfirewall.CfnFirewall(this, 'EgressFirewall', {
      firewallName: `${this.stackName}-egress`,
      firewallPolicyArn: policy.attrFirewallPolicyArn,
      vpcId: this.vpc.vpcId,
      // One endpoint, not one per zone, for the same reason there is one NAT gateway: a path
      // carrying 0.39 MB per run does not need zonal independence, and an endpoint is billed at
      // $0.395 an hour whether or not a task is running. Two of them came to roughly $577 a
      // month standing, against $288 for one — on a fleet whose entire argument is that it costs
      // nothing when idle. Leaving the two in place while writing that argument was not
      // defensible once the rate was looked up.
      subnetMappings: [{ subnetId: inspection[0].subnetId }],
      // Left on: a deployment that can delete the thing constraining egress is one mistake away
      // from an open fleet.
      deleteProtection: true,
      firewallPolicyChangeProtection: true,
      subnetChangeProtection: true,
    });
  }

  private publishOutputs(): void {
    new CfnOutput(this, 'VpcId', { value: this.vpc.vpcId });
    new CfnOutput(this, 'TaskSubnetIds', {
      value: this.vpc.selectSubnets(this.taskSubnets).subnetIds.join(','),
      description: 'Private subnets for tasks and functions; egress passes the firewall',
    });
    new CfnOutput(this, 'TaskSecurityGroupId', {
      value: this.taskSecurityGroup.securityGroupId,
    });
  }
}
