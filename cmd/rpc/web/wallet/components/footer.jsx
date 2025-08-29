import Container from "react-bootstrap/Container";
import { DiscordIcon, TwitterIcon } from "@/components/svg_icons";

const socials = [
  {
    url: "https://discord.gg/pNcSJj7Wdh",
    icon: <DiscordIcon />,
  },
  {
    url: "https://x.com/CNPYNetwork",
    icon: <TwitterIcon />,
  },
];

const Footer = () => {
  return (
    <footer className="footer-light">
      <Container>
        {/* Use map to render social icons dynamically */}
        {socials.map((social, index) => (
          <a key={index} href={social.url} target="_blank">
            {" "}
            {/* Add a unique key prop */}
            <div className="nav-social-icon">{social.icon}</div>
          </a>
        ))}
      </Container>
    </footer>
  );
};

export default Footer;
